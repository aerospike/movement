package com.aerospike.graph.move;

import com.aerospike.graph.move.common.tinkerpop.PluginServiceFactory;
import com.aerospike.graph.move.process.operations.Generate;
import com.aerospike.graph.move.process.operations.Migrate;
import com.aerospike.graph.move.util.ConfigurationUtil;
import com.aerospike.graph.move.util.ErrorUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Plugin implements PluginServiceFactory.PluginInterface {

    public static final String NAME = "movement";
    private final Configuration config;
    private final Object system;
    private Plugin(Configuration config, Object system) {
        this.config = config;
        this.system = system;
    }
    public static Plugin open(Configuration config, Object system) {
        return new Plugin(config, system);
    }
    public static Map<String, List<String>> api() {
        Map<String, List<String>> api = new HashMap<>() {{
            put(Generate.class.getSimpleName(), List.of());
            put(Migrate.class.getSimpleName(), List.of());
        }};
        return api;
    }
    @Override
    public Iterator<?> call(String operation, Map<String, Object> params) {
        Configuration callConfig = ConfigurationUtil.configurationWithOverrides(config, params);
        if (operation.equals(Generate.class.getSimpleName())) {
            CLI.run(ConfigurationUtil.configurationWithOverrides(callConfig, Generate.getBaseConfig()));
            return IteratorUtils.of(true);
        } else if (operation.equals(Migrate.class.getSimpleName())) {
            CLI.run(ConfigurationUtil.configurationWithOverrides(callConfig, Migrate.getBaseConfig()));
            return IteratorUtils.of(true);
        } else {
            throw ErrorUtil.unimplemented();
        }
    }
    @Override
    public String toString() {
        return NAME;
    }
}
