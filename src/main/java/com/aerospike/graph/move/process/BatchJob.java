package com.aerospike.graph.move.process;

import com.aerospike.graph.move.CLI;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.encoding.format.csv.GraphCSVEncoder;
import com.aerospike.graph.move.output.file.DirectoryOutput;
import com.aerospike.graph.move.util.ConfigurationUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;

public class BatchJob {


    public static DirectoryOutput.Config CONFIG = new DirectoryOutput.Config();

    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String BATCH_TASK_NAMES = "batch.task.names";
            public static final String BATCH_TASK_SETTINGS = "batch.task.settings";

        }

        public Map<String, String> taskSettings(final String taskName, final Configuration config) {
            return IteratorUtils.set(IteratorUtils.concat(config.getKeys(), DEFAULTS.keySet().iterator())).stream()
                    .filter(key -> key.startsWith(Keys.BATCH_TASK_SETTINGS + "." + taskName + "."))
                    .collect(Collectors.toMap(key -> key, key -> config.getString(key)));
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, "1000");
            put(DirectoryOutput.Config.Keys.ENCODER, GraphCSVEncoder.class.getName());
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, "/tmp/generate");
            put(DirectoryOutput.Config.Keys.VERTEX_OUTPUT_DIRECTORY, "/tmp/generate/vertices");
            put(DirectoryOutput.Config.Keys.EDGE_OUTPUT_DIRECTORY, "/tmp/generate/edges");
            put(DirectoryOutput.Config.Keys.BUFFER_SIZE_KB, "4096");
        }};
    }

    final List<Configuration> configs;
    final Map<String, Map<String, Object>> overrides = new HashMap<>();

    private BatchJob(List<Configuration> configs) {
        this.configs = configs;
    }

    public BatchJob withOverrides(Map<String, Map<String, Object>> overrides) {
        this.overrides.putAll(overrides);
        return this;
    }

    public static BatchJob of(Configuration... configs) {
        return new BatchJob(Arrays.stream(configs).collect(Collectors.toList()));
    }

    public void run() {
        if (!overrides.isEmpty()) {
            final Configuration config = configs.get(0);
            for (Map<String, Object> override : overrides.values()) {
                CLI.run(ConfigurationUtil.configurationWithOverrides(config, override));
            }
        } else {
            for (Configuration config : configs) {
                CLI.run(config);
            }
        }
    }
}
