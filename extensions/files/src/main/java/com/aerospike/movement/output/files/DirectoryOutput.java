/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.output.files;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.emitter.files.DirectoryEmitter;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.test.mock.emitter.MockEmitable;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.files.FileUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

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

    public static final String VERTICES = "vertices";
    public static final String EDGES = "edges";
    public static final String MOCK = "mock";

    public static String getTypeDirectory(final Class type) {
        if (EmittedVertex.class.isAssignableFrom(type)) {
            return VERTICES;
        } else if (EmittedEdge.class.isAssignableFrom(type)) {
            return EDGES;
        } else if (MockEmitable.class.isAssignableFrom(type)) {
            return MOCK;
        } else {
            throw new RuntimeException("Type unknown to file output : " + type);
        }
    }


    @Override
    public OutputWriter writer(final Class type, final String label) {
        return fileWriters.computeIfAbsent(type.toString(), it -> new HashMap<>()).computeIfAbsent(label, labelKey -> {
            final Configuration writerConfig;

            final Path typePath = resolveOrCreate(path, getTypeDirectory(type)).toAbsolutePath();
            resolveOrCreate(typePath, label);
            writerConfig = ConfigurationUtil.configurationWithOverrides(config, Map.of(
                    Config.Keys.OUTPUT_DIRECTORY, typePath.toString()
            ));
            final OutputWriter outputWriter = SplitFileLineOutput.create(label, encoder, getMetric(label), writerConfig);
            outputWriter.init();
            return outputWriter;
        });
    }

    @Override
    public Emitter reader(final Runtime.PHASE phase, final Class type, final Optional<String> label, final Configuration callerConfig) {
        final Configuration readerConfig = ConfigurationUtil.configurationWithOverrides(config, new MapConfiguration(new HashMap<>() {{
            put(DirectoryEmitter.Config.Keys.LABEL, label);
            put(DirectoryEmitter.Config.Keys.BASE_PATH, Path.of((String) CONFIG.getOrDefault(Config.Keys.VERTEX_OUTPUT_DIRECTORY, config)).getParent().toString());
            put(DirectoryEmitter.Config.Keys.PHASE_ONE_SUBDIR, Path.of((String) CONFIG.getOrDefault(Config.Keys.VERTEX_OUTPUT_DIRECTORY, config)).getFileName().toString());
            put(DirectoryEmitter.Config.Keys.PHASE_TWO_SUBDIR, Path.of((String) CONFIG.getOrDefault(Config.Keys.EDGE_OUTPUT_DIRECTORY, config)).getFileName().toString());
            put(ConfigurationBase.Keys.PHASE_OVERRIDE, phase.name());
        }}));

        return DirectoryEmitter.open(readerConfig);
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
                .reduce(RuntimeUtil::mapReducer)
                .orElse(new HashMap<>() {{
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
