package com.aerospike.movement.runtime.core.local;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.util.core.*;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class LocalParallelStreamRuntime implements Runtime {
    public final static List<Output> outputs = Collections.synchronizedList(new ArrayList<>());
    public final static List<Emitter> emitters = Collections.synchronizedList(new ArrayList<>());
    public final static List<Encoder> encoders = Collections.synchronizedList(new ArrayList<>());
    public final static List<Decoder> decoders = Collections.synchronizedList(new ArrayList<>());
    public final static AtomicReference<OutputIdDriver> outputIdDriver = new AtomicReference<>();
    public final static AtomicReference<WorkChunkDriver> workChunkDriver = new AtomicReference<>();
    public final static Map<String, Class<? extends Task>> taskAliases = new ConcurrentHashMap<>();

    public static int getBatchSize(final Configuration config) {
        return Integer.parseInt(CONFIG.getOrDefault(Config.Keys.BATCH_SIZE, config));
    }

    public static void halt() {
        INSTANCE.customThreadPool.shutdown();
        INSTANCE.close();
    }


    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String THREADS = "runtime.threads";
            public static final String DROP_OUTPUT = "runtime.dropOutput";
            public static final String DELAY_MS = "runtime.outputStallTimeMs";
            public static final String BATCH_SIZE = "driver.batchSize";


        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.THREADS, String.valueOf(RuntimeUtil.getAvailableProcessors()));
            put(Keys.DROP_OUTPUT, "false");
            put(Keys.DELAY_MS, "100");
            put(Keys.BATCH_SIZE, "1000");
        }};
    }

    public static Config CONFIG = new Config();

    private static final String id = UUID.randomUUID().toString();
    public static LocalParallelStreamRuntime INSTANCE;
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ErrorHandler errorHandler;
    private final ForkJoinPool customThreadPool;
    private final Configuration config;


    private LocalParallelStreamRuntime(final Configuration config) {
        this.config = config;
        this.customThreadPool = new ForkJoinPool(Integer.parseInt(CONFIG.getOrDefault(Config.Keys.THREADS, config)));
        this.errorHandler = RuntimeUtil.loadErrorHandler(this, config);
    }

    public LocalParallelStreamRuntime init(Configuration config) {
        if (initialized.get()) {
            throw new RuntimeException("Runtime already initialized");
        }
        close();
        return (LocalParallelStreamRuntime) getInstance(config);
    }

    public static Runtime open(Configuration config) {
        return getInstance(config);
    }

    public static Runtime getInstance(Configuration config) {
        if (initialized.compareAndSet(false, true)) {
            INSTANCE = new LocalParallelStreamRuntime(config);
        }
        return INSTANCE;
    }

    @Override
    public Iterator<RunningPhase> runPhases(final List<PHASE> phases, final Configuration config) {
        return phases.stream().map(phase -> runPhase(phase, config)).iterator();
    }

    @Override
    public RunningPhase runPhase(final PHASE phase, final Configuration config) {
        // Id iterator drives the process, it may be bounded to a specific purpose, return unordered or sequential ids, or be unbounded
        // If it is unbounded, it simply drives the stream and the emitter impl it is driving should end the process when finished.
        return executePhase(phase, customThreadPool, this, setPhaseConfiguration(phase, config));
    }


    private Configuration setPhaseConfiguration(final PHASE phase, final Configuration config) {
        return ConfigurationUtil.configurationWithOverrides(config, Map.of(ConfigurationBase.Keys.PHASE, phase.name()));
    }

    @Override
    public Iterator<Map<String, Object>> runTask(final Task task) {
        return Task.run(task);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LocalParallelStreamRuntime: \n");
        sb.append("  Threads: ").append(customThreadPool.getParallelism()).append("\n");
        sb.append("  Loaded: ").append("\n");
        return sb.toString();
    }


    public void close() {
        closeStatic();
    }

    public static void closeStatic() {
        getAllLoaded().forEachRemaining(RuntimeUtil::closeWrap);
        emitters.clear();
        outputs.clear();
        encoders.clear();
        decoders.clear();
        outputIdDriver.set(null);
        workChunkDriver.set(null);
        ErrorHandler.trigger.set(null);
        initialized.set(false);
        INSTANCE = null;
    }

    public static Iterator<Loadable> getAllLoaded() {
        return (Iterator<Loadable>) IteratorUtils.wrap(
                IteratorUtils.concat(RuntimeUtil.lookup(Emitter.class),
                        RuntimeUtil.lookup(Encoder.class),
                        RuntimeUtil.lookup(Decoder.class),
                        RuntimeUtil.lookup(Output.class),
                        RuntimeUtil.lookup(OutputIdDriver.class),
                        RuntimeUtil.lookup(WorkChunkDriver.class)));
    }


    private ForkJoinTask<?> submit(final ForkJoinPool pool, final Runnable task) {
        return pool.submit(task);
    }


    private static List<Pipeline> setupPipelines(final int count, final PHASE phase, final Configuration config) {
        return IntStream.range(0, count).mapToObj(pipelineId ->
                Pipeline.create(pipelineId, phase, config)).collect(Collectors.toList());
    }

    private static void processEmitable(final Emitable emitable, final Output output) {
        IteratorUtils.iterate(RuntimeUtil.walk(emitable.emit(output), output));
    }

    private static void driveIndividualThreadSync(final PHASE phase,
                                                  final WorkChunkDriver driver,
                                                  final Emitter emitter,
                                                  final Output output,
                                                  final Runnable completionHandler,
                                                  final Handler<Throwable> errorHandler) {
        try {
            final Iterator<Emitable> emitableIterator = emitter.stream(driver, phase).iterator();
            while (emitableIterator.hasNext()) {
                processEmitable(emitableIterator.next(), output);
            }
        } catch (Exception e) {
            errorHandler.handle(e, output);
        }
        completionHandler.run();
    }

    private static RunningPhase executePhase(final Runtime.PHASE phase,
                                             final ForkJoinPool customThreadPool,
                                             final LocalParallelStreamRuntime runtime,
                                             final Configuration config) {
        Output.init(phase, config);
        Emitter.init(phase, config);
        Encoder.init(phase, config);
        Decoder.init(phase, config);
        ((WorkChunkDriver) RuntimeUtil.lookupOrLoad(WorkChunkDriver.class, config)).init(config);
        try {
            final List<Pipeline> pipelines = setupPipelines(customThreadPool.getParallelism(), phase, config);
            final ParallelStreamProcessor processor = ParallelStreamProcessor.create(pipelines, config, phase);
            final ForkJoinTask<?> task = runtime.submit(customThreadPool, processor);
            return RunningPhase.of(task, pipelines, phase, config);
        } catch (Exception e) {
            runtime.errorHandler.handleError(e, phase);
            throw new RuntimeException(e);
        }
    }

    public static class ParallelStreamProcessor implements Runnable {
        private final List<Pipeline> pipelines;
        private final Configuration config;
        private final PHASE phase;
        private final ErrorHandler errorHandler;

        private ParallelStreamProcessor(final List<Pipeline> pipelines, final PHASE phase, final Configuration config) {
            this.pipelines = pipelines;
            this.config = config;
            this.phase = phase;
            this.errorHandler = RuntimeUtil.getErrorHandler(this, config);
        }

        public static ParallelStreamProcessor create(final List<Pipeline> pipelines, final Configuration config, final PHASE phase) {
            return new ParallelStreamProcessor(pipelines, phase, config);
        }

        private static class RuntimeErrorHandler implements Handler<Throwable> {
            private final WaitGroup waitGroup;
            private final ErrorHandler upstream;

            private RuntimeErrorHandler(final WaitGroup waitGroup, final ErrorHandler upstream) {
                this.waitGroup = waitGroup;
                this.upstream = upstream;
            }

            public static RuntimeErrorHandler create(final WaitGroup waitGroup, final ErrorHandler upstream) {
                return new RuntimeErrorHandler(waitGroup, upstream);
            }

            @Override
            public void handle(final Throwable t, final Object... context) {
                waitGroup.done();
                upstream.handleError(t, waitGroup);
            }
        }

        //Make your elementIterators size equal to your threadpool size
        @Override
        public void run() {
            final WaitGroup waitGroup = WaitGroup.of(pipelines.size());
            pipelines.stream().parallel().forEach(pipeline -> {
                final Emitter emitter = pipeline.getEmitter();
                final Output output = pipeline.getOutput();
                final WorkChunkDriver driver = (WorkChunkDriver) RuntimeUtil.lookupOrLoad(WorkChunkDriver.class, config);
                final ErrorHandler errorHandler = RuntimeUtil.loadErrorHandler(this, config);
                try {
                    LocalParallelStreamRuntime.driveIndividualThreadSync(phase,
                            driver,
                            emitter,
                            output,
                            () -> {
                                RuntimeUtil.closeWrap(pipeline);
                                waitGroup.done();
                            }, RuntimeErrorHandler.create(waitGroup, errorHandler)
                    );
                } catch (Exception e) {
                    errorHandler.handleError(e, pipeline);
                    throw e;
                }
            });
            try {
                waitGroup.await();
                RuntimeUtil.closeAllInstancesOfLoadable(WorkChunkDriver.class);
                RuntimeUtil.unload(WorkChunkDriver.class);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}



