package com.aerospike.graph.move.common.tinkerpop;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PluginServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    private final Configuration config;
    private final Object plugin;

    public static final String OPEN = "open";
    public static final String API = "api";
    public static final String OPERATION = "operation";
    public static final String CALL = "call";

    public interface PluginInterface {
        Iterator<?> call(String operation, Map<String, Object> params);
    }


    private final Map<String, List<String>> apiMethods;

    private PluginServiceFactory(final Configuration config, final Object plugin, final Map<String, List<String>> apiMethods) {
        this.config = config;
        this.plugin = plugin;
        this.apiMethods = apiMethods;
    }

    public static PluginServiceFactory create(final Configuration config, final String pluginClassName, final Object system) {
        Object plugin = load(pluginClassName, config, system);
        Map<String, List<String>> apiMethods = getApiMethods(plugin);
        return new PluginServiceFactory(config, plugin, apiMethods);
    }

    @Override
    public String getName() {
        return plugin.toString();
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return null;
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return this;
    }

    @Override
    public Type getType() {
        return null;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        validateParams(params);
        String operation = (String) params.get(OPERATION);
        if (operation.equals("API")) {
            return (CloseableIterator<R>) IteratorUtils.map(apiMethods.keySet().iterator(), key -> new AbstractMap.SimpleEntry<>(key, apiMethods.get(key).iterator()));
        }
        return CloseableIterator.of(call(operation, params));
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }


    private Iterator<?> call(final String operation, final Map<String, Object> params) {
        try {
            return (Iterator<?>) plugin.getClass().getMethod(CALL, String.class, Map.class).invoke(plugin, operation, params);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void validateParams(final Map<String, Object> params) {
        if (!params.containsKey(OPERATION)) {
            throw new IllegalArgumentException("Missing required parameter: " + OPERATION);
        }
        String operation = (String) params.get(OPERATION);
        if (!apiMethods.containsKey(operation) || operation.equals(API)) {
            throw new IllegalArgumentException("Invalid operation: " + operation);
        }
        Set<String> argNamesPassed = params.keySet();
        if (!argNamesPassed.containsAll(apiMethods.get(operation))) {
            throw new IllegalArgumentException("Missing required parameters: " + Arrays.toString(apiMethods.keySet().stream().filter(it -> !argNamesPassed.contains(it)).toArray()));
        }
    }

    private static Map<String, List<String>> getApiMethods(Object plugin) {
        try {
            return (Map<String, List<String>>) plugin.getClass().getMethod(API).invoke(plugin);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Object load(final String className, final Configuration config, final Object system) {
        try {
            return Class.forName(className).getMethod(OPEN, Configuration.class, Object.class).invoke(null, config, system);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
