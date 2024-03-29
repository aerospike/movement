/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.plugin.tinkerpop;

import com.aerospike.movement.plugin.Plugin;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.util.core.Pair;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public class PluginServiceFactory<I> implements Service.ServiceFactory<I, Map<String, Object>>, Service<I, Map<String, Object>> {
    private final Configuration config;
    private final Plugin plugin;
    public static final String OPEN = "open";
    public static final String API = "api";
    public static final String HELP = "help";
    public static final String CALL = "call";
    public static final String TASK_STATUS = "TaskStatus";
    public static final String WAIT_TASK = "WaitTask";
    public static final String TASK_ID = "taskId";


    private final String serviceName;
    private final Graph system;

    private PluginServiceFactory(final Task task, final Plugin plugin, final Graph system, final Configuration config) {
        this.serviceName = task.getClass().getSimpleName();
        this.config = config;
        this.plugin = plugin;
        this.system = system;
    }

    public static PluginServiceFactory create(final Task serviceTask, final Plugin plugin, final Graph system, final Configuration config) {
        return new PluginServiceFactory(serviceTask, Optional.ofNullable(plugin).orElseThrow(), system, config);
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
    public Service<I, Map<String, Object>> createService(final boolean isStart, final Map params) {
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
    public CloseableIterator<Map<String, Object>> execute(final ServiceCallContext ctx, final Map params) {
        if (serviceName.equals(TASK_STATUS)) {
            return CloseableIterator.of(IteratorUtils.of(Task.StatusMonitor.from(UUID.fromString((String) params.get(TASK_ID))).status(true)));
        }
        if (serviceName.equals(WAIT_TASK)) {
            RuntimeUtil.waitTask(UUID.fromString((String) params.get(TASK_ID)));
            return CloseableIterator.of(Collections.emptyIterator());

        }
        final Configuration overriddenConfig = ConfigUtil.withOverrides(config, params);
        if (Optional.ofNullable((String) params.get(HELP)).isPresent()) {
            final Optional<List<String>> helpForService = Optional.ofNullable(plugin.api().get(serviceName));
            return (CloseableIterator<Map<String, Object>>) helpForService
                    .orElseThrow(() -> new IllegalArgumentException("could not get api for " + serviceName));
        }
        Pair<LocalParallelStreamRuntime, Iterator<Object>> result = plugin.runTask(serviceName, overriddenConfig)
                .orElseThrow(() -> new RuntimeException("could not run task for " + serviceName));
        Iterator<Object> iterator = (Iterator<Object>) result.right;
        LocalParallelStreamRuntime runtime = (LocalParallelStreamRuntime) result.left;
        UUID id = (UUID) iterator.next();
        Task.StatusMonitor monitor = Task.StatusMonitor.from(id);
        return CloseableIterator.of(IteratorUtils.concat(IteratorUtils.of(Map.of("id", id)), new Iterator<Map<String, Object>>() {
            @Override
            public boolean hasNext() {
                return monitor.isRunning();
            }

            @Override
            public Map<String, Object> next() {
                Map<String, Object> x = monitor.status(true);
                x.put("id", id);
                return x;
            }
        }));
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
