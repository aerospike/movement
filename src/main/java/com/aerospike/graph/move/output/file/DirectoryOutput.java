package com.aerospike.graph.move.output.file;

import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.encoding.format.csv.GraphCSVEncoder;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.output.OutputWriter;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.IOUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
            public static final String VERTEX_OUTPUT_DIRECTORY = "output.vertexDirectory";
            public static final String EDGE_OUTPUT_DIRECTORY = "output.edgeDirectory";
            public static final String BUFFER_SIZE_KB = "output.bufferSizeKB";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.ENTRIES_PER_FILE, "1000");
            put(Keys.ENCODER, GraphCSVEncoder.class.getName());
            put(Keys.OUTPUT_DIRECTORY, "/tmp/generate");
            put(Keys.VERTEX_OUTPUT_DIRECTORY, "/tmp/generate/vertices");
            put(Keys.EDGE_OUTPUT_DIRECTORY, "/tmp/generate/edges");
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

    protected DirectoryOutput(final Path root, final int entriesPerFile, final Encoder<String> encoder, final Configuration config) {
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
