/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.local;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.logging.core.Level;
import com.aerospike.movement.runtime.core.Pipeline;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.movement.util.core.runtime.RuntimeUtil.closeWrap;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class LocalParallelStreamRuntime implements Runtime {
    public static Level logLevel = Level.INFO;
    public final static List<Output> outputs = Collections.synchronizedList(new ArrayList<>());
    public final static List<Emitter> emitters = Collections.synchronizedList(new ArrayList<>());
    public final static List<Encoder> encoders = Collections.synchronizedList(new ArrayList<>());
    public final static List<Decoder> decoders = Collections.synchronizedList(new ArrayList<>());
    public final static List<Task> tasks = Collections.synchronizedList(new ArrayList<>());

    public final static AtomicReference<OutputIdDriver> outputIdDriver = new AtomicReference<>();
    public final static AtomicReference<WorkChunkDriver> workChunkDriver = new AtomicReference<>();
    public final static Map<String, Class<? extends Task>> taskAliases = new ConcurrentHashMap<>();
    public final static AtomicReference<Optional<RunningPhase>> runningPhase = new AtomicReference<>();
    public final static Map<UUID, Map<String, Object>> runningTasks = new ConcurrentHashMap<>();

    public final static Map<String, Runnable> cleanupCallbacks = new ConcurrentHashMap<>();

    public static void halt() {
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
            return ConfigUtil.getKeysFromClass(Config.Keys.class);
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
            put(Keys.BATCH_SIZE, "100");
        }};
    }

    public static Config CONFIG = new Config();

    public static LocalParallelStreamRuntime INSTANCE;
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ErrorHandler errorHandler;
    private final Configuration config;
    public int parallelIsm;


    private LocalParallelStreamRuntime(final Configuration config) {
        this.config = config;
        parallelIsm = 1 + Integer.parseInt(CONFIG.getOrDefault(Config.Keys.THREADS, config));
        String cfg = CONFIG.getOrDefault(Config.Keys.THREADS, config);
        this.errorHandler = RuntimeUtil.getErrorHandler(this, config);
    }


    public LocalParallelStreamRuntime init(final Configuration config) {
        if (initialized.get()) {
            throw errorHandler.handleError(new RuntimeException("Runtime already initialized"));
        }
        close();
        return (LocalParallelStreamRuntime) getInstance(config);
    }

    public static Runtime open(final Configuration config) {
        return getInstance(config);
    }

    public static Runtime getInstance(final Configuration config) {
        synchronized (LocalParallelStreamRuntime.class) {
            if (initialized.get()) {
                return INSTANCE;
            } else {
                initialized.set(true);
                INSTANCE = new LocalParallelStreamRuntime(config);
            }
        }
        return INSTANCE;
    }

    @Override
    public RunningPhase runPhase(final PHASE phase, final Configuration config) {
        return runPhase(phase, setupPipelines(parallelIsm, phase, config), config);
    }


    @Override
    public Iterator<RunningPhase> runPhases(final List<PHASE> phases, final Configuration config) {
        return phases.stream().map(phase -> runPhase(phase, ConfigUtil.withOverrides(config, new HashMap<>() {{
            put(ConfigurationBase.Keys.PHASE, phase.name());
            put(ConfigurationBase.Keys.INTERNAL_PHASE_INDICATOR, phase.name());
        }}))).iterator();
    }


    public RunningPhase runPhase(final PHASE phase, final List<Pipeline> pipelines, final Configuration config) {
        Optional<RunningPhase> x = Optional.of(executePhase(phase, this, pipelines, setPhaseConfiguration(phase, config)));
        runningPhase.set(x);
        return x.get();
    }


    private Configuration setPhaseConfiguration(final PHASE phase, final Configuration config) {
        return ConfigUtil.withOverrides(config, Map.of(ConfigurationBase.Keys.INTERNAL_PHASE_INDICATOR, phase.name()));
    }

    public static final String TASK_ID_KEY = "taskId";
    public static final String TASK_KEY = "task";
    public static final String FUTURE_KEY = "future";
    public static final String PHASE_REF_KEY = "phaseRef";
    public static final String STATUS_MONITOR_KEY = "statusMonitor";

    @Override
    public Iterator<?> runTask(final Task task) {
        final UUID taskId = UUID.randomUUID();
        final AtomicReference<Optional<RunningPhase>> rpRef = new AtomicReference<>(Optional.empty());

        Semaphore sem = new Semaphore(0);
        final Future<Map<String, Object>> fut = CompletableFuture.supplyAsync(() -> {
            final Map<String, Object> phaseResults = new HashMap<>();
            Task.run(task).forEachRemaining(rp -> {
                rpRef.set(Optional.of(rp));
                sem.release();
                rp.get();

                phaseResults.put(rp.phase.name(), rp.status().next());
            });
            CompletableFuture.supplyAsync(() -> {
                RuntimeUtil.waitTask(taskId);
                task.onComplete(true);
                return null;
            });
            return phaseResults;
        });
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        runningTasks.put(taskId, new HashMap<>() {{
            put(TASK_ID_KEY, taskId);
            put(TASK_KEY, task);
            put(FUTURE_KEY, fut);
            put(PHASE_REF_KEY, rpRef);
        }});
        runningTasks.get(taskId).put(STATUS_MONITOR_KEY, new Task.StatusMonitor(taskId));
        return List.of(taskId).iterator();
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LocalParallelStreamRuntime: \n");
        sb.append("  Threads: ").append((String) CONFIG.getOrDefault(Config.Keys.THREADS, config)).append("\n");
        sb.append("  Loaded: ").append("\n");
        return sb.toString();
    }


    public void close() {
        closeStatic();
    }

    public void cancel() {
        runningPhase.get().ifPresent(rp -> rp.close());

        close();
    }

    public static void closeStatic() {
        Optional.ofNullable(runningPhase.get()).map(p -> p.get()).ifPresent(p -> {
            p.get();
            p.getPipelines().stream().forEach(pipeline -> closeWrap(pipeline));
            p.getOutputs().stream().map(o -> closeWrap(o));
            p.close();

        });

        getAllLoaded().forEachRemaining(it -> {
            if (!it.isClosed()) closeWrap(it);
        });
        cleanupCallbacks.forEach((k, v) -> v.run());

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
        List<Loadable> results = new ArrayList<>();
        results.addAll((List<Loadable>) RuntimeUtil.lookup(Emitter.class));
        results.addAll((List<Loadable>) RuntimeUtil.lookup(Encoder.class));
        results.addAll((List<Loadable>) RuntimeUtil.lookup(Decoder.class));
        results.addAll((List<Loadable>) RuntimeUtil.lookup(Decoder.class));
        results.addAll((List<Loadable>) RuntimeUtil.lookup(Output.class));
        results.addAll((List<Loadable>) RuntimeUtil.lookup(OutputIdDriver.class));
        results.addAll((List<Loadable>) RuntimeUtil.lookup(WorkChunkDriver.class));
        return results.iterator();
    }


    private static List<Pipeline> setupPipelines(final int count, final PHASE phase, final Configuration config) {
        return IntStream
                .range(0, count)
                .mapToObj(pipelineId ->
                        Pipeline.create(pipelineId, phase, config))
                .collect(Collectors.toList());
    }

    private static RunningPhase executePhase(final Runtime.PHASE phase,
                                             final LocalParallelStreamRuntime runtime,
                                             final List<Pipeline> pipelines,
                                             final Configuration config) {
        RuntimeUtil.loadWorkChunkDriver(config).init(config);
        pipelines.forEach(it -> ((Loadable) it.getEmitter()).init(config));
        Output.init(phase, config);
        Emitter.init(phase, config);
        Encoder.init(phase, config);
        Decoder.init(phase, config);


        try {
            final ParallelStreamProcessor processor = ParallelStreamProcessor.create(pipelines, config, runtime, phase);
            return RunningPhase.execute(processor, pipelines, phase, config);
        } catch (Exception e) {
            throw runtime.errorHandler.handleFatalError(e, phase, config);
        }
    }
}



