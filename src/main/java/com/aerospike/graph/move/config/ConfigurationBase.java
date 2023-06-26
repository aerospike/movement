package com.aerospike.graph.move.config;

import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.encoding.format.csv.GraphCSV;
import com.aerospike.graph.move.encoding.format.tinkerpop.TraversalEncoder;
import com.aerospike.graph.move.output.file.DirectoryOutput;
import com.aerospike.graph.move.output.tinkerpop.TraversalOutput;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class ConfigurationBase {
    public static class Keys {
        public static final String EMITTER = "emitter";
        public static final String DECODER = "decoder";
        public static final String ENCODER = "encoder";
        public static final String OUTPUT = "output";
    }

    public String getOrDefault(Configuration config, String key) {
        return Optional.ofNullable(config.containsKey(key) ? config.getString(key) : getDefaults().get(key)).orElseThrow(() ->
                new RuntimeException("Missing required configuration key: " + key));
    }

    public abstract Map<String, String> getDefaults();

    public static Configuration getCSVSampleConfiguration(final String schemaLocation, final String outputDirectory) {
        return new MapConfiguration(new HashMap<>() {{
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 100L);
            put(Generator.Config.Keys.SCHEMA_FILE, schemaLocation);
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, outputDirectory);
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
            put(DirectoryOutput.Config.Keys.ENCODER, GraphCSV.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, DirectoryOutput.class.getName());
            put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, 100);
        }});
    }

    public static Configuration getGraphSampleConfiguration(final String schemaLocation) {
        return new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.OUTPUT, TraversalOutput.class.getName());
            put(ConfigurationBase.Keys.ENCODER, TraversalEncoder.class.getName());
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 100L);
            put(Generator.Config.Keys.SCHEMA_FILE, schemaLocation);
            put(Generator.Config.Keys.ROOT_VERTEX_LABEL, "account");
            put(TraversalEncoder.Config.Keys.HOST, "localhost");
            put(TraversalEncoder.Config.Keys.PORT, 8182);
            put(TraversalEncoder.Config.Keys.CLEAR, true);
        }});
    }

    public static Configuration getRemoteSampleConfiguration(final String schemaLocation) {
        return new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.OUTPUT, TraversalOutput.class.getName());
            put(ConfigurationBase.Keys.ENCODER, TraversalEncoder.class.getName());
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 100L);
            put(Generator.Config.Keys.SCHEMA_FILE, schemaLocation);
            put(Generator.Config.Keys.ROOT_VERTEX_LABEL, "account");
            put(TraversalEncoder.Config.Keys.HOST, "localhost");
            put(TraversalEncoder.Config.Keys.PORT, 8182);
            put(TraversalEncoder.Config.Keys.CLEAR, true);
        }});
    }

    public static String configurationToPropertiesFormat(Configuration config) {
        final StringBuilder sb = new StringBuilder();
        config.getKeys().forEachRemaining(key -> {
            sb.append(key).append("=").append(config.getProperty(key)).append("\n");
        });
        return sb.toString();
    }

}
