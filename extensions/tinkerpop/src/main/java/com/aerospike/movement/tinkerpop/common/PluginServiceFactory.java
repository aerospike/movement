/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.plugin.Plugin;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public class PluginServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    private final Configuration config;
    private final Plugin plugin;
    public static final String OPEN = "open";
    public static final String API = "api";
    public static final String HELP = "help";
    public static final String CALL = "call";

    private final String serviceName;
    private final Graph system;

    private PluginServiceFactory(final Task task, final Plugin plugin, final Graph system, final Configuration config) {
        this.serviceName = task.getClass().getSimpleName();
        this.config = config;
        this.plugin = plugin;
        this.system = system;
    }

    public static PluginServiceFactory create(final Task serviceTask, final Plugin plugin, final Graph system, final Configuration config) {
        return new PluginServiceFactory(serviceTask, plugin, system, config);
    }

    @Override
    public String getName() {
        return this.serviceName;
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
        final Configuration overriddenConfig = ConfigurationUtil.configurationWithOverrides(config, params);
//        validateParams(overriddenConfig);
        if (Optional.ofNullable((String) params.get(HELP)).isPresent()) {
            final Optional<List<String>> helpForService = Optional.ofNullable(plugin.api().get(serviceName));
            return (CloseableIterator<R>) helpForService
                    .orElseThrow(() -> new IllegalArgumentException("could not get api for " + serviceName));
        }
        return CloseableIterator.of((Iterator<R>) plugin.runTask(serviceName, overriddenConfig)
                .orElseThrow(() -> new RuntimeException("could not run task for " + serviceName)));
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

    private void validateParams(final Configuration config) {
        final Set<String> configKeys = IteratorUtils.set(IteratorUtils.concat(config.getKeys(), plugin.getConfigurationMeta().defaults().getKeys()));
        final Map<String, List<String>> pluginAPI = plugin.api();
        final List<String> taskAPI = Optional.ofNullable(pluginAPI.get(serviceName))
                .orElseThrow(() -> new IllegalArgumentException("could not get api for " + serviceName));
        if (!configKeys.containsAll(taskAPI)) {
            throw new IllegalArgumentException("Missing required parameters: " + Arrays.toString(plugin.api().get(serviceName).stream().filter(it -> !configKeys.contains(it)).toArray()));
        }
    }

}
