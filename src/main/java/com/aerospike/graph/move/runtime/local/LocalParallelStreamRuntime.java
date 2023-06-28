package com.aerospike.graph.move.runtime.local;

import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.encoding.Decoder;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.process.Job;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Runtime.getRuntime;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class LocalParallelStreamRuntime implements Runtime {
    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String THREADS = "runtime.threads";
            public static final String DROP_OUTPUT = "runtime.dropOutput";
            public static final String OUTPUT_STARTUP_DELAY_MS = "runtime.outputStallTimeMs";

        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Config.Keys.THREADS, String.valueOf(getRuntime().availableProcessors()));
            put(Config.Keys.DROP_OUTPUT, "false");
            put(Config.Keys.OUTPUT_STARTUP_DELAY_MS, "100");
        }};
    }

    public static Config CONFIG = new Config();

    private enum DelayType {
        IO_THREAD_INIT
    }
    private static final String id = UUID.randomUUID().toString();
    private static LocalParallelStreamRuntime INSTANCE;
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private final DefaultErrorHandler errorHandler;
    private final ForkJoinPool customThreadPool;
    private final Configuration config;
    private final Map<Long, Output> outputMap = new java.util.concurrent.ConcurrentHashMap<>();


    public LocalParallelStreamRuntime(final Configuration config) {
        this.config = config;
        this.customThreadPool = new ForkJoinPool(Integer.parseInt(CONFIG.getOrDefault(config, Config.Keys.THREADS)));
        this.errorHandler = new DefaultErrorHandler(config, id);
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


    //Distributed runtime uses this to provide specific elements to load for this JVM
    @Override
    public Map.Entry<ForkJoinTask, List<Output>> phaseOne(final Iterator<List<Object>> idSupplier) {
        return processStream(PHASE.ONE, idSupplier, customThreadPool, this, config);
    }

    @Override
    public RunningPhase phaseOne() {
        // Id iterator drives the process, it may be bounded to a specific purpose, return unordered or sequential ids, or be unbounded
        // If it is unbounded, it simply drives the stream and the emitter impl it is driving should end the process when finished.
        final Emitter emitter = RuntimeUtil.loadEmitter(config);
        return RunningPhase.of(processStream(Runtime.PHASE.ONE, emitter.getDriverForPhase(PHASE.ONE), customThreadPool, this, config));
    }


    @Override
    public RunningPhase phaseTwo(Iterator<List<Object>> idSupplier) {
        return RunningPhase.of(processStream(Runtime.PHASE.TWO, idSupplier, customThreadPool, this, config));
    }

    @Override
    public RunningPhase phaseTwo() {
        Emitter e = RuntimeUtil.loadEmitter(config);
        return phaseTwo(e.getDriverForPhase(PHASE.TWO));
    }

    @Override
    public Optional<String> submitJob(final Job job) {
        throw ErrorUtil.unimplemented();
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LocalParallelStreamRuntime: \n");
        sb.append("  Threads: ").append(customThreadPool.getParallelism()).append("\n");
        sb.append("  Output: ").append(outputMap.values().stream().map(Output::toString).collect(Collectors.joining("\n"))).append("\n");
        return sb.toString();
    }


    public void close() {
        outputMap.values().forEach(Output::close);
    }

    public Map<Long, Output> getOutputMap() {
        return outputMap;
    }

    public List<Long> getOutputVertexMetrics() {
        return List.copyOf(outputMap.values().stream().map(Output::getVertexMetric).collect(Collectors.toList()));
    }

    public List<Long> getOutputEdgeMetrics() {
        return List.copyOf(outputMap.values().stream().map(Output::getEdgeMetric).collect(Collectors.toList()));
    }


    private void delay(final DelayType type) {
        switch (type) {
            case IO_THREAD_INIT:
                try {
                    Thread.sleep(Long.parseLong(CONFIG.getOrDefault(config, Config.Keys.OUTPUT_STARTUP_DELAY_MS)));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                break;
        }
    }

    private ForkJoinTask<?> submit(final ForkJoinPool pool, final Runnable task) throws ExecutionException, InterruptedException {
        return pool.submit(task);
    }

    private Map.Entry<Emitter, Output> createNewEmitterOutputPair(final int id) {
        final Emitter emitter = RuntimeUtil.loadEmitter(config);
        final Output output = RuntimeUtil.loadOutput(config);
        outputMap.put((long) id, output);
        return new AbstractMap.SimpleEntry<>(emitter, output);
    }

    private List<Map.Entry<Stream<Emitable>, Output>> createAndSetupEmitterToOutputConnections(
            final Iterator<List<Object>> idSupplier,
            final PHASE phase) {

        return IntStream.range(0, customThreadPool.getParallelism()).mapToObj(threadNumber -> {
            delay(DelayType.IO_THREAD_INIT);
            return createNewEmitterOutputPair(threadNumber);
        }).collect(Collectors.toList()).stream().map(emitterOutputPair -> {
            final Emitter emitter = emitterOutputPair.getKey();
            final Output output = emitterOutputPair.getValue();
            return new AbstractMap.SimpleEntry<>(emitter.stream(IteratorUtils.flatMap(idSupplier, List::iterator), phase), output);
        }).collect(Collectors.toList());
    }

    private static void processEmitable(final Emitable emitable, final Output output) {
        IteratorUtils.iterate(RuntimeUtil.walk(emitable.emit(output), output));
    }

    static void driveIndividualThreadSync(final Stream<Emitable> input, final Output output) {
        input.iterator().forEachRemaining(emitable -> {
            processEmitable(emitable, output);
        });
    }

    private static Map.Entry<ForkJoinTask, List<Output>> processStream(final PHASE phase,
                                                                       final Iterator<List<Object>> idSupplier,
                                                                       final ForkJoinPool customThreadPool,
                                                                       final LocalParallelStreamRuntime runtime,
                                                                       final Configuration config) {

        Output.init(phase.value(), config);
        Emitter.init(phase.value(), config);
        Encoder.init(phase.value(), config);
        Decoder.init(phase.value(), config);
        final ForkJoinTask<?> task;
        final List<Map.Entry<Stream<Emitable>, Output>> emitterConnections;
        try {
            emitterConnections = runtime.createAndSetupEmitterToOutputConnections(idSupplier, phase);
            final ParallelStreamProcessor processor = ParallelStreamProcessor.create(emitterConnections, config);
            task = runtime.submit(customThreadPool, processor);
        } catch (Exception e) {
            runtime.errorHandler.handle(e);
            throw new RuntimeException(e);
        }
        return new AbstractMap.SimpleEntry<>(task, emitterConnections.stream().map(entry -> entry.getValue()).collect(Collectors.toList()));
    }

    public static class ParallelStreamProcessor implements Runnable {
        private final List<Map.Entry<Stream<Emitable>, Output>> initalizedEmitterOutputPairs;
        private final Configuration config;

        private ParallelStreamProcessor(final List<Map.Entry<Stream<Emitable>, Output>> initializedStreamOutputPairs, final Configuration config) {
            this.initalizedEmitterOutputPairs = initializedStreamOutputPairs;
            this.config = config;

        }

        public static ParallelStreamProcessor create(final List<Map.Entry<Stream<Emitable>, Output>> initializedStreamOutputPairs, final Configuration config) {
            return new ParallelStreamProcessor(initializedStreamOutputPairs, config);
        }


        //Make your elementIterators size equal to your threadpool size
        @Override
        public void run() {
            initalizedEmitterOutputPairs.stream().parallel().forEach(emitterStreamOutputPair -> {
                final Stream<Emitable> elementStream = emitterStreamOutputPair.getKey();
                final Output output = emitterStreamOutputPair.getValue();
                DefaultErrorHandler errorHandler = new DefaultErrorHandler(config, String.format("%s:%d", LocalParallelStreamRuntime.class.getName(), Thread.currentThread().getId()));
                try {
                    LocalParallelStreamRuntime.driveIndividualThreadSync(elementStream, output);
                } catch (Exception e) {
                    errorHandler.handle(e);
                    throw e;
                }
            });
        }
    }

}



