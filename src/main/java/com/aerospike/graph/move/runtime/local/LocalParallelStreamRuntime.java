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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Runtime.getRuntime;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class LocalParallelStreamRuntime implements Runtime {
    public static Config CONFIG = new Config();
    private static final String id = UUID.randomUUID().toString();

    // 1 per JVM
    private static AtomicReference<LocalParallelStreamRuntime> INSTANCE = new AtomicReference<LocalParallelStreamRuntime>();
    private final DefaultErrorHandler errorHandler;

    public static Runtime getInstance(Configuration config) {
        if (INSTANCE.get() == null) {
            INSTANCE.set(new LocalParallelStreamRuntime(config));
        }
        return INSTANCE.get();
    }

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


    private final ForkJoinPool customThreadPool;
    private final Configuration config;
    private Map<Long, Output> outputMap = new java.util.concurrent.ConcurrentHashMap<>();

    public LocalParallelStreamRuntime(final Configuration config) {
        this.config = config;
        this.customThreadPool = new ForkJoinPool(Integer.parseInt(CONFIG.getOrDefault(config, Config.Keys.THREADS)));
        this.errorHandler = new DefaultErrorHandler(config, id);
    }

    public static Runtime open(Configuration config) {
        return new LocalParallelStreamRuntime(config);
    }


    //Distributed runtime uses this to provide specific elements to load for this JVM
    @Override
    public void initialPhase(final Iterator<Object> idSupplier) {
        processStream(Runtime.PHASE.ONE, idSupplier, customThreadPool, this, config);
    }

    @Override
    public void initialPhase() {
        // Id iterator drives the process, it may be bounded to a specific purpose, return unordered or sequential ids, or be unbounded
        // If it is unbounded, it simply drives the stream and the emitter impl it is driving should end the process when finished.
        initialPhase(RuntimeUtil.loadEmitter(config).phaseOneIterator());
    }


    @Override
    public void completionPhase(Iterator<Object> idSupplier) {
        processStream(Runtime.PHASE.TWO, idSupplier, customThreadPool, this, config);
    }

    @Override
    public void completionPhase() {
        completionPhase(RuntimeUtil.loadEmitter(config).phaseTwoIterator());
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


    @Override
    public Optional<String> submitJob(Job job) {
        throw ErrorUtil.unimplemented();
    }


    public void processEdgeStream(Iterator<List<Object>> idSupplier) {
        //@todo
        completionPhase();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LocalParallelStreamRuntime: \n");
        sb.append("  Threads: ").append(customThreadPool.getParallelism()).append("\n");
        sb.append("  Output: ").append(outputMap.values().stream().map(Output::toString).collect(Collectors.joining("\n"))).append("\n");
        return sb.toString();
    }

    private enum DelayType {
        IO_THREAD_INIT
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

    private Object submit(final ForkJoinPool pool, final Runnable task) throws ExecutionException, InterruptedException {
        return pool.submit(task).get();
    }

    private Map.Entry<Emitter, Output> createNewEmitterOutputPair(final int id) {
        final Emitter emitter = RuntimeUtil.loadEmitter(config);
        final Output output = RuntimeUtil.loadOutput(config);
        outputMap.put((long) id, output);
        return new AbstractMap.SimpleEntry<>(emitter, output);
    }

    private List<Map.Entry<Stream<Emitable>, Output>> createAndSetupEmitterToOutputConnections(final Iterator<Object> idSupplier, final PHASE phase) {

        return IntStream.range(0, customThreadPool.getParallelism()).mapToObj(threadNumber -> {
            delay(DelayType.IO_THREAD_INIT);
            return createNewEmitterOutputPair(threadNumber);
        }).collect(Collectors.toList()).stream().map(emitterOutputPair -> {
            final Emitter emitter = emitterOutputPair.getKey();
            final Output output = emitterOutputPair.getValue();
            final Stream<Emitable> elementStream = phase.equals(Runtime.PHASE.ONE) ?
                    emitter.withIdSupplier(idSupplier).phaseOneStream() :
                    emitter.withIdSupplier(idSupplier).phaseTwoStream();
            return new AbstractMap.SimpleEntry<>(elementStream, output);
        }).collect(Collectors.toList());
    }

    private static void processEmitable(Emitable emitable, Output output) {
        IteratorUtils.iterate(RuntimeUtil.walk(emitable.emit(output), output));
    }

    static void driveIndividualThreadSync(final Stream<Emitable> input, final Output output) {
        input.iterator().forEachRemaining(emitable -> {
            processEmitable(emitable, output);
        });
    }

    private static void processStream(final PHASE phase,
                                      final Iterator<Object> idSupplier,
                                      final ForkJoinPool customThreadPool,
                                      final LocalParallelStreamRuntime runtime,
                                      final Configuration config) {


        Output.init(phase.value(), config);
        Emitter.init(phase.value(), config);
        Encoder.init(phase.value(), config);
        Decoder.init(phase.value(), config);

        try {
            runtime.submit(customThreadPool,
                    ParallelStreamProcessor.create(
                            runtime.createAndSetupEmitterToOutputConnections(idSupplier, phase)));
        } catch (Exception e) {
            runtime.errorHandler.handle(e);
        }
    }

    public static class ParallelStreamProcessor implements Runnable {
        private final List<Map.Entry<Stream<Emitable>, Output>> initializedStreamOutputPairs;

        private ParallelStreamProcessor(List<Map.Entry<Stream<Emitable>, Output>> initializedStreamOutputPairs) {
            this.initializedStreamOutputPairs = initializedStreamOutputPairs;

        }

        public static ParallelStreamProcessor create(final List<Map.Entry<Stream<Emitable>, Output>> initializedStreamOutputPairs) {
            return new ParallelStreamProcessor(initializedStreamOutputPairs);
        }


        //Make your elementIterators size equal to your threadpool size
        @Override
        public void run() {
            initializedStreamOutputPairs.stream().parallel().forEach(emitterStreamOutputPair -> {
                final Stream<Emitable> elementStream = emitterStreamOutputPair.getKey();
                final Output output = emitterStreamOutputPair.getValue();
                LocalParallelStreamRuntime.driveIndividualThreadSync(elementStream, output);
            });
        }
    }

}



