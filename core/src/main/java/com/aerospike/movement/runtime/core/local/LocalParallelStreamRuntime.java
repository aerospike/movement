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
import com.aerospike.movement.runtime.core.Pipeline;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
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
    public final ForkJoinPool customThreadPool;
    private final Configuration config;


    private LocalParallelStreamRuntime(final Configuration config) {
        this.config = config;
        this.customThreadPool = new ForkJoinPool(Integer.parseInt(CONFIG.getOrDefault(Config.Keys.THREADS, config)));
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
        return ConfigurationUtil.configurationWithOverrides(config, Map.of(ConfigurationBase.Keys.INTERNAL_PHASE_INDICATOR, phase.name()));
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


    private static List<Pipeline> setupPipelines(final int count, final PHASE phase, final Configuration config) {
        return IntStream
                .range(0, count)
                .mapToObj(pipelineId ->
                        Pipeline.create(pipelineId, phase, config))
                .collect(Collectors.toList());
    }

    private static RunningPhase executePhase(final Runtime.PHASE phase,
                                             final ForkJoinPool customThreadPool,
                                             final LocalParallelStreamRuntime runtime,
                                             final Configuration config) {
        Output.init(phase, config);
        Emitter.init(phase, config);
        Encoder.init(phase, config);
        Decoder.init(phase, config);


        try {
            final List<Pipeline> pipelines = setupPipelines(customThreadPool.getParallelism(), phase, config);
            final ParallelStreamProcessor processor = ParallelStreamProcessor.create(pipelines, config, runtime, phase);
            return RunningPhase.execute(processor, pipelines, phase, config);
        } catch (Exception e) {
            throw runtime.errorHandler.handleFatalError(e, phase, config);
        }
    }
}



