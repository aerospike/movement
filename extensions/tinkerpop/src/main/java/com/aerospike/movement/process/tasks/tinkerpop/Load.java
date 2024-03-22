/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.process.tasks.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Load extends Task {

    static {
        RuntimeUtil.registerTaskAlias(Load.class.getSimpleName(), Load.class);
    }

    public static final String RECURSIVE_DIR_TRAVERSAL_CLASS_NAME = "com.aerospike.movement.emitter.files.RecursiveDirectoryTraversalDriver";

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            final Map<String, String> newConfig = new HashMap<>();
            config.entrySet().stream().forEach(entry -> {
                newConfig.put(entry.getKey(), (String) entry.getValue());
            });
            newConfig.put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, RECURSIVE_DIR_TRAVERSAL_CLASS_NAME);
            newConfig.put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO, RECURSIVE_DIR_TRAVERSAL_CLASS_NAME);
            newConfig.put(ConfigurationBase.Keys.EMITTER, "com.aerospike.movement.emitter.files.DirectoryEmitter");
//            newConfig.put(ConfigurationBase.Keys.ENCODER, TinkerPopGraphEncoder.class.getName());
            newConfig.put(ConfigurationBase.Keys.DECODER, "com.aerospike.movement.encoding.files.csv.GraphCSVDecoder");
//            newConfig.put(ConfigurationBase.Keys.OUTPUT, TinkerPopGraphOutput.class.getName());
            return newConfig;
        }

        @Override
        public List<String> getKeys() {
            return ConfigUtil.getKeysFromClass(MockOutput.Config.Keys.class);
        }


        public static class Keys {

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

    public Load(Configuration config) {
        super(Config.INSTANCE, config);
    }

    public static Load open(Configuration config) {
        return new Load(config);
    }

    @Override
    public Configuration getConfig(Configuration config) {
        return ConfigUtil.withOverrides(new MapConfiguration(Config.INSTANCE.defaultConfigMap()), config);
    }

    @Override
    public Map<String, Object> getMetrics() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean succeeded() {
        return false;
    }

    @Override
    public boolean failed() {
        return false;
    }

    @Override
    public List<Runtime.PHASE> getPhases() {
        return List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO);
    }

    @Override
    public void init(final Configuration config) {

    }

    @Override
    public void onClose()  {

    }
}
