package com.aerospike.movement.plugin;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Handler;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.util.core.Pair;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import org.apache.commons.configuration2.Configuration;

import java.util.*;

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
            ConfigUtil.getConfigurationMeta(taskClass).getKeys();
            x.put(shortName, ConfigUtil.getConfigurationMeta(taskClass).getKeys());
        });
        return x;
    }

    public Map<String, List<String>> api(final Task task) {
        return Map.of(task.getClass().getSimpleName(), task.getConfigurationMeta().getKeys());
    }


    public Iterator<?> call(final String taskName, final Configuration config) {
        Iterator<?> x = LocalParallelStreamRuntime.open(config)
                .runTask(Task.getTaskByAlias(taskName, config));
        return x;
    }

    public List<Class<Task>> listTasks() {
        return new ArrayList<>(RuntimeUtil.findAvailableSubclasses(Task.class));
    }

    public Iterator<Object> listComponents() {
        return RuntimeUtil.findAvailableSubclasses(Loadable.class).stream().map(it -> (Object) it.toString()).iterator();
    }

    public Optional<Pair<LocalParallelStreamRuntime, Iterator<Object>>> runTask(final String taskName, final Configuration externalConfig) {
        Task task = Task.getTaskByAlias(taskName, externalConfig);
        return runTask(task, task.getConfig(externalConfig));
    }

    public Optional<Pair<LocalParallelStreamRuntime, Iterator<Object>>> runTask(final Task task, final Configuration config) {
        final Configuration taskBaseConfig = ConfigUtil.getConfigurationMeta(task.getClass()).defaults();
        final Configuration taskConfig = ConfigUtil.withOverrides(taskBaseConfig, config);

        task.init(config);
        final LocalParallelStreamRuntime taskRuntime = (LocalParallelStreamRuntime) LocalParallelStreamRuntime.open(taskConfig);
        task.addCompletionHandler("close runtime", (e, context) -> taskRuntime.close());
        try {
            return Optional.of(Pair.of(taskRuntime, (Iterator<Object>) taskRuntime.runTask(task)));
        } catch (Exception e) {
            errorHandler.handleFatalError(e, task);
            return Optional.empty();
        }
    }

}
