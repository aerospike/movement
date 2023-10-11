package com.aerospike.movement.plugin;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorHandler;
import com.aerospike.movement.util.core.Handler;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.CloseableIterator;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.stream.Collectors;

public abstract class Plugin extends Loadable implements PluginInterface {
    protected final ErrorHandler errorHandler;

    protected Plugin(final ConfigurationBase base, final Configuration config) {
        super(base, config);
        this.errorHandler = RuntimeUtil.getErrorHandler(this, config);
    }

    public static class BaseTasks {
        public static final String LIST_TASKS = "listTasks";
        public static final String LIST_COMPONENTS = "listComponents";
    }

    public Map<String, List<String>> api() {
        final HashMap<String, List<String>> x = new HashMap<>() {{
            put(BaseTasks.LIST_TASKS, List.of());
            put(BaseTasks.LIST_COMPONENTS, List.of());
        }};
        RuntimeUtil.getTasks().entrySet().stream().forEach(entry -> {
            final String shortName = entry.getKey();
            final Class<? extends Task> taskClass = entry.getValue();
            ConfigurationUtil.getConfigurationMeta(taskClass).getKeys();
            x.put(shortName, ConfigurationUtil.getConfigurationMeta(taskClass).getKeys());
        });
        return x;
    }

    public Map<String, List<String>> api(final Task task) {
        return Map.of(task.getClass().getSimpleName(), task.getConfigurationMeta().getKeys());
    }

    protected Iterator<?> call(final String taskName, final Configuration config) {
        return LocalParallelStreamRuntime.open(config)
                .runTask(Task.getTaskByAlias(taskName, config));
    }

    public List<Class<Task>> listTasks() {
        return new ArrayList<>(RuntimeUtil.findAvailableSubclasses(Task.class));
    }

    public Iterator<Object> listComponents() {
        return RuntimeUtil.findAvailableSubclasses(Loadable.class).stream().map(it -> (Object) it.toString()).iterator();
    }

    public Optional<Iterator<Object>> runTask(final String taskName, final Configuration config) {
        return runTask(Task.getTaskByAlias(taskName, config), config);
    }

    public Optional<Iterator<Object>> runTask(final Task task, final Configuration config) {
        final Configuration taskBaseConfig = ConfigurationUtil.getConfigurationMeta(task.getClass()).defaults();
        final Configuration taskConfig = ConfigurationUtil.configurationWithOverrides(taskBaseConfig, config);

        task.init(config);
        try {
            return Optional.of((Iterator<Object>) IteratorUtils.wrap(LocalParallelStreamRuntime.open(taskConfig).runTask(task)));
        } catch (Exception e) {
            errorHandler.handleError(e, task);
            return Optional.empty();
        }
    }

}
