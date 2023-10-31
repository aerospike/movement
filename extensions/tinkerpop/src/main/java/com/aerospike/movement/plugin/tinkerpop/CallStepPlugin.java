/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.plugin.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.logging.core.LoggerFactory;
import com.aerospike.movement.plugin.Plugin;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.tinkerpop.common.PluginServiceFactory;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class CallStepPlugin extends Plugin {


    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String,Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }


    public static final String NAME = "movement";
    private final Configuration config;

    private CallStepPlugin(Configuration config) {
        super(Config.INSTANCE, config);
        this.config = config;
    }

    public static Plugin open(Configuration config) {
        final Plugin plugin = new CallStepPlugin(config);
        return plugin;
    }


    @Override
    public void plugInto(final Object system) {
        if (!Graph.class.isAssignableFrom(system.getClass()))
            throw errorHandler.error("cannot setup movement plugin, valid graph to plug into");
        final Graph graph = (Graph) system;
        final List<String> taskInfo = RuntimeUtil.findAvailableTaskAliases();

        for (final String entry : taskInfo) {
            LoggerFactory.withContext(this).debug("Registering task: %s", entry);
            final Task task = (Task) RuntimeUtil.lookupOrLoad(RuntimeUtil.getTaskClassByAlias(entry), config);
            final PluginServiceFactory psf = PluginServiceFactory.create(task, this, graph, config);
            graph.getServiceRegistry().registerService(psf);
        }
    }

    @Override
    public Iterator<?> call(String taskName, Configuration passed) {
        return super.call(taskName, passed);
    }

    @Override
    public String toString() {
        return NAME;
    }

    @Override
    public void init(final Configuration config) {

    }

    @Override
    public void close() throws Exception {

    }
}
