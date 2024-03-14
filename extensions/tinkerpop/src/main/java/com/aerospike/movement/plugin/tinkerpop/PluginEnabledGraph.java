/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.plugin.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.plugin.Plugin;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PluginEnabledGraph {
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
            return ConfigUtil.getKeysFromClass(CallStepPlugin.Config.Keys.class);
        }


        public static class Keys {
            public static final String GRAPH_IMPL = "plugin.enabled.graph.impl";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }

    public static Graph open(Configuration config) {
        final Graph graph = (Graph) RuntimeUtil.openClassRef(Config.INSTANCE.getOrDefault(Config.Keys.GRAPH_IMPL, config), config);
        final Plugin plugin = CallStepPlugin.open(config);
        RuntimeUtil.getLogger().info("plugin: " + plugin.getClass().getName());
        plugin.plugInto(graph);
        RuntimeUtil.getLogger().info(graph.traversal().call("--list").toList());
        return graph;
    }
}
