package com.aerospike.graph.generator.output.file;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.emitter.generated.GeneratedVertex;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.encoder.format.csv.CSVEncoder;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.output.OutputWriter;
import com.aerospike.graph.generator.runtime.CapturedError;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.IOUtil;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class DirectoryOutput implements Output {
    private static ConcurrentHashMap<String, Encoder> encoderCache = new ConcurrentHashMap<>();

    private final Configuration config;
    public static Config CONFIG = new Config();
    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String ENTRIES_PER_FILE = "output.entriesPerFile";
            public static final String ENCODER = "encoder";
            public static final String OUTPUT_DIRECTORY = "output.directory";
            public static final String BUFFER_SIZE_KB = "output.bufferSizeKB";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.ENTRIES_PER_FILE, "1000");
            put(Keys.ENCODER, CSVEncoder.class.getName());
            put(Keys.OUTPUT_DIRECTORY, "/tmp/generate");
            put(Keys.BUFFER_SIZE_KB, "4096");
        }};
    }


    private final Path vertexPath;
    private final Path edgePath;
    private final int entriesPerFile;
    private final Encoder<String> encoder;
    private final AtomicLong edgeMetric;
    private final AtomicLong vertexMetric;
    private Path root;
    private final Map<String, SplitFileLineOutput> vertexOutputs = new ConcurrentHashMap<>();
    private final Map<String, SplitFileLineOutput> edgeOutputs = new ConcurrentHashMap<>();

    private static Path resolveOrCreate(final Path root, final String name) {
        Path path = root.resolve(name);
        if (!path.toFile().exists()) {
            if (!path.toFile().mkdirs())
                throw new RuntimeException("Could not create directory " + path);
        }
        return path;
    }

    public DirectoryOutput(final Path root, final int entriesPerFile, final Encoder<String> encoder, final Configuration config) {
        this.encoder = encoder;
        this.entriesPerFile = entriesPerFile;
        this.root = root;
        this.vertexPath = resolveOrCreate(root, "vertices");
        this.edgePath = resolveOrCreate(root, "edges");
        this.edgeMetric = new AtomicLong(0);
        this.vertexMetric = new AtomicLong(0);
        this.config = config;
    }

    public static DirectoryOutput open(Configuration config) {
        final Encoder encoder = (Encoder) encoderCache.computeIfAbsent(CONFIG.getOrDefault(config, Config.Keys.ENCODER), key -> (Encoder) RuntimeUtil.loadEncoder(config));
        final int entriesPerFile = Integer.valueOf(CONFIG.getOrDefault(config, Config.Keys.ENTRIES_PER_FILE));
        final String outputDirectory = CONFIG.getOrDefault(config, Config.Keys.OUTPUT_DIRECTORY);
        return new DirectoryOutput(Path.of(outputDirectory), entriesPerFile, encoder, config);
    }

    private int WRITES_BEFORE_FLUSH = 10;

    @Override
    public Stream<Optional<CapturedError>> writeVertexStream(final Stream<EmittedVertex> vertexStream) {
        return vertexStream.map(generatedVertex -> {
            Optional<CapturedError> result = Optional.empty();
            try {
                vertexWriter(generatedVertex.label()).writeVertex(generatedVertex);
            } catch (Exception e) {
                result = Optional.of(new CapturedError(e, new GeneratedVertex.GeneratedVertexId(generatedVertex.id().getId())));
            }
            return result;
        });
    }

    @Override
    public Stream<Optional<CapturedError>> writeEdgeStream(final Stream<EmittedEdge> edgeStream) {
        return edgeStream.map(generatedEdge -> {
            Optional<CapturedError> result = Optional.empty();
            try {
                edgeWriter(generatedEdge.label()).writeEdge(generatedEdge);
            } catch (Exception e) {
                result = Optional.of(new CapturedError(e, new GeneratedVertex.GeneratedVertexId(generatedEdge.fromId().getId())));
            }
            return result;
        });
    }


    @Override
    public OutputWriter edgeWriter(final String label) {
        return edgeOutputs.computeIfAbsent(label, labelKey -> {
            final Path path = edgePath.resolve(labelKey);
            return new SplitFileLineOutput(labelKey,
                    path,
                    WRITES_BEFORE_FLUSH,
                    encoder,
                    entriesPerFile,
                    edgeMetric,
                    config);
        });
    }

    @Override
    public OutputWriter vertexWriter(final String label) {
        return vertexOutputs.computeIfAbsent(label, labelKey -> {
            final Path path = vertexPath.resolve(labelKey);
            return new SplitFileLineOutput(labelKey,
                    path,
                    WRITES_BEFORE_FLUSH,
                    encoder,
                    entriesPerFile,
                    vertexMetric,
                    config);
        });
    }

    public Path getRootPath() {
        return root;
    }

    public Long getVertexMetric() {
        return vertexMetric.get();
    }

    @Override
    public void close() {
        IteratorUtils.concat(vertexOutputs.values().iterator(), edgeOutputs.values().iterator())
                .forEachRemaining(it -> {
                    it.close();
                });
    }

    @Override
    public void dropStorage() {
        LoggerFactory.getLogger(DirectoryOutput.class).info("Dropping storage at {}", root);
        IOUtil.recursiveDelete(root);
        root.toFile().mkdirs();
    }

    public Long getEdgeMetric() {
        return edgeMetric.get();
    }

    @Override
    public String toString() {
        return "DirectoryOutput: " + "\n" +
                "  vertexPath: " + vertexPath + "\n" +
                "  edgePath: " + edgePath + "\n" +
                "  entriesPerFile: " + entriesPerFile + "\n" +
                "  encoder: " + encoder + "\n" +
                "  edgesWritten: " + edgeMetric + "\n" +
                "  verticesWritten: " + vertexMetric + "\n" +
                "  root: " + root + "\n" +
                "  vertexOutputs: " + vertexOutputs + "\n" +
                "  edgeOutputs: " + edgeOutputs + "\n";
    }
}
