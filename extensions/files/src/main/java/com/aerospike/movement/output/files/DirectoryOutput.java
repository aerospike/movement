package com.aerospike.movement.output.files;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.graph.EmittedEdge;
import com.aerospike.movement.emitter.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.test.mock.emitter.MockEmitable;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.files.FileUtil;
import org.apache.commons.configuration2.Configuration;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class DirectoryOutput extends Loadable implements Output {

    @Override
    public void init(final Configuration config) {

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
            public static final String ENCODER = "encoder";
            public static final String OUTPUT_DIRECTORY = "output.directory";
            public static final String VERTEX_OUTPUT_DIRECTORY = "output.vertexDirectory";
            public static final String EDGE_OUTPUT_DIRECTORY = "output.edgeDirectory";


            public static final String BUFFER_SIZE_KB = "output.bufferSizeKB";
            public static final String WRITES_BEFORE_FLUSH = "output.writesBeforeFlush";
            public static final String ENTRIES_PER_FILE = "output.entriesPerFile";

            public static final String DIRECTORY = "output.directory";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.OUTPUT_DIRECTORY, "/tmp/generate");
            put(Keys.VERTEX_OUTPUT_DIRECTORY, "/tmp/generate/vertices");
            put(Keys.EDGE_OUTPUT_DIRECTORY, "/tmp/generate/edges");
            put(Keys.ENTRIES_PER_FILE, "1000");
            put(Keys.BUFFER_SIZE_KB, "4096");
            put(Keys.WRITES_BEFORE_FLUSH, "1000");
        }};
    }

    public static Config CONFIG = new Config();
    private final Configuration config;


    private static ConcurrentHashMap<String, Encoder> encoderCache = new ConcurrentHashMap<>();

    private final Path path;
    private final Encoder<String> encoder;

    private final Map<String, AtomicLong> metricsByOutputType = new ConcurrentHashMap<>();
    private final Map<String, Map<String, OutputWriter>> fileWriters = new ConcurrentHashMap<>();

    protected DirectoryOutput(final Path path,
                              final Encoder<String> encoder,
                              final Configuration config) {
        super(CONFIG, config);
        this.encoder = encoder;
        this.path = path;
        this.config = config;
    }

    public static DirectoryOutput open(Configuration config) {
        final Encoder<String> encoder = (Encoder<String>) encoderCache.computeIfAbsent(CONFIG.getOrDefault(Config.Keys.ENCODER, config),
                key -> (Encoder<String>) RuntimeUtil.loadEncoder(config));
        final String outputDirectory = CONFIG.getOrDefault(Config.Keys.OUTPUT_DIRECTORY, config);
        return new DirectoryOutput(Path.of(outputDirectory), encoder, config);
    }

    @Override
    public OutputWriter writer(final Class type, final Object label) {
        return fileWriters.computeIfAbsent(type.toString(), it -> new HashMap<>()).computeIfAbsent(label.toString(), labelKey -> {
            final Configuration writerConfig;
            if (EmittedVertex.class.isAssignableFrom(type)) {
                writerConfig = ConfigurationUtil.configurationWithOverrides(config, Map.of(
                        Config.Keys.DIRECTORY, resolveOrCreate(path, "vertices").toString()
                ));
                resolveOrCreate(resolveOrCreate(path, "vertices"), label.toString());
            } else if (EmittedEdge.class.isAssignableFrom(type)) {
                writerConfig = ConfigurationUtil.configurationWithOverrides(config, Map.of(
                        Config.Keys.DIRECTORY, resolveOrCreate(path, "edges").toString()
                ));
                resolveOrCreate(resolveOrCreate(path, "edges"), label.toString());
            } else if (MockEmitable.class.isAssignableFrom(type)) {
                writerConfig = ConfigurationUtil.configurationWithOverrides(config, Map.of(
                        Config.Keys.DIRECTORY, resolveOrCreate(path, "mock").toString()
                ));
                resolveOrCreate(resolveOrCreate(path, "mock"), label.toString());
            } else {
                throw new RuntimeException("Type unknown to file output : " + type);
            }

            final OutputWriter outputWriter = SplitFileLineOutput.create(label.toString(), encoder, getMetric(label), writerConfig);
            outputWriter.init();
            return outputWriter;
        });
    }

    private AtomicLong getMetric(final Object label) {
        return metricsByOutputType.computeIfAbsent(label.toString(), it -> new AtomicLong());
    }

    @Override
    public void close() {
        fileWriters.values().forEach(it -> {
            it.values().forEach(output -> {
                output.close();
            });
        });
        encoder.close();
        SplitFileLineOutput.fileIncr.set(0);
    }

    @Override
    public void dropStorage() {
        FileUtil.recursiveDelete(path);
        path.toFile().mkdirs();
    }

    public Map<String, Object> getMetrics() {
        return metricsByOutputType
                .entrySet().stream()
                .map(it ->
                        Map.of(it.getKey(), (Object) it.getValue().get()))
                .reduce((a, b) -> {
                    try {
                        Map<String, Object> x = new HashMap<>();
                        x.putAll(a);
                        x.putAll(b);
                         return x;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).orElse(new HashMap<>() {{
                    put("status", 0L);
                }});
    }

    private static Path resolveOrCreate(final Path root, final String name) {
        final Path path = root.resolve(name);
        synchronized (DirectoryOutput.class) {
            if (!path.toFile().exists()) {
                if (!path.toFile().mkdirs())
                    throw new RuntimeException("Could not create directory " + path);
            }
        }
        return path;
    }


    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DirectoryOutput: ").append("\n");
        stringBuilder.append("  path: ").append(path).append("\n");
        stringBuilder.append("  encoder: ").append(encoder).append("\n");
        stringBuilder.append("  metrics: ").append("\n");
        metricsByOutputType.forEach((key, value) -> {
            stringBuilder.append("    ").append(key).append(": ").append(value.get()).append("\n");
        });
        stringBuilder.append("  files: ").append("\n");
        fileWriters.forEach((key, value) -> {
            stringBuilder.append("    ").append(key).append(": ").append(value.keySet().stream().map(String::toString)).append("\n");
        });
        return stringBuilder.toString();
    }
}
