package com.aerospike.graph.generator.runtime;

import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.emitter.Emitter;
import com.aerospike.graph.generator.emitter.generated.Generator;
import com.aerospike.graph.generator.emitter.generated.StitchMemory;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.util.BatchedIterator;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.RuntimeUtil;
import com.aerospike.graph.generator.util.SyncronizedBatchIterator;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.lang.Runtime.getRuntime;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class LocalParallelStreamRuntime implements Runtime {
    public static Config CONFIG = new Config();

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


    private final StitchMemory memory;
    private final ForkJoinPool customThreadPool;
    private final Configuration config;
    private Map<Long, Output> outputMap = new java.util.concurrent.ConcurrentHashMap<>();

    public LocalParallelStreamRuntime(final StitchMemory memory,
                                      final Configuration config) {
        this.memory = memory;
        this.config = config;
        this.customThreadPool = new ForkJoinPool(Integer.parseInt(CONFIG.getOrDefault(config, Config.Keys.THREADS)));
    }

    private void handleError(Exception e) {
        System.err.println(e);
    }

    public void processVertexStream() {

        final int threads = customThreadPool.getParallelism();
        final long rootVertexIdStart = Long.parseLong(Generator.CONFIG.getOrDefault(config, Generator.Config.Keys.ROOT_VERTEX_ID_START));
        final long rootVertexIdEnd = Long.parseLong(Generator.CONFIG.getOrDefault(config, Generator.Config.Keys.ROOT_VERTEX_ID_END));
        final long rootVertexIdRange = rootVertexIdEnd - rootVertexIdStart;
        final long rootVertexIdRangePerThread = rootVertexIdRange / threads;
        final Iterator<List<Long>> idSupplier = new SyncronizedBatchIterator<>(
                LongStream.range(rootVertexIdEnd + 1, Long.MAX_VALUE).iterator(), 1000);

        Map<Output, Stream<EmittedVertex>> vertexIterators =
                IntStream.range(0, threads).mapToObj(threadNumber -> {
                    try {
                        Thread.sleep(Long.parseLong(CONFIG.getOrDefault(config, Config.Keys.OUTPUT_STARTUP_DELAY_MS)));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    final Emitter emitter = RuntimeUtil.loadEmitter(config);
                            final Output output = RuntimeUtil.loadOutput(config);
                            outputMap.put((long) threadNumber, output);
                            final long startId = rootVertexIdStart + (threadNumber * rootVertexIdRangePerThread);
                            final long endId = startId + rootVertexIdRangePerThread;
                            return new AbstractMap.SimpleEntry<>(output, emitter
                                    .withIdSupplier(new BatchedIterator<>(idSupplier))
                                    .vertexStream(startId, endId));
                        }
                ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (Boolean.parseBoolean(CONFIG.getOrDefault(config, Config.Keys.DROP_OUTPUT))) {
            RuntimeUtil.loadOutput(config).dropStorage();
        }
        try {
            customThreadPool.submit(
                    () -> vertexIterators.entrySet().stream().parallel().forEach(outputProducerPair -> {
                        final Output output = outputProducerPair.getKey();
                        final Stream<EmittedVertex> vertexIterator = outputProducerPair.getValue();
                        vertexIterator.iterator().forEachRemaining(generatedVertex -> {
                            try {
                                IteratorUtils.iterate(RuntimeUtil.walk(generatedVertex.emit(output), output));
                            } catch (Exception e) {
                                handleError(e);
                            }
                        });
                    })).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    public void processEdgeStream() {
        return;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LocalParallelStreamRuntime: \n");
        sb.append("  Threads: ").append(customThreadPool.getParallelism()).append("\n");
        sb.append("  Output: ").append(outputMap.values().stream().map(Output::toString).collect(Collectors.joining("\n"))).append("\n");
        return sb.toString();
    }
}
