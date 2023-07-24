package com.aerospike.movement.process.core;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Task extends Loadable {
    private final String id;
    protected final Configuration config;
    protected final AtomicBoolean isRunning = new AtomicBoolean(false);

    private static final Map<String, Task> jobs = new ConcurrentHashMap<>();

    protected Task(final ConfigurationBase configurationMeta, final Configuration config) {
        super(configurationMeta, config);
        this.id = UUID.randomUUID().toString();
        this.config = config;
    }

    public static Task getTaskByAlias(final String operation, final Configuration config) {
        Class<? extends Task> task = RuntimeUtil.getTaskClassByAlias(operation);
        return (Task) RuntimeUtil.lookupOrLoad(task, config);
    }

    public Configuration getConfig() {
        return this.config;
    }

    public static Iterator<Map<String, Object>> run(final Task task) {
        final Configuration jobConfig = task.getConfig();
        final LocalParallelStreamRuntime runtime = (LocalParallelStreamRuntime) LocalParallelStreamRuntime.getInstance(jobConfig);
        final List<Runtime.PHASE> p = task.getPhases();
        return p.stream()
                .map(it -> runtime.runPhase(it, jobConfig))
                .flatMap(runningPhase -> IteratorUtils.stream(runningPhase.status())).iterator();
    }


    public abstract Map<String, Object> getMetrics();

    public boolean isRunning() {
        return isRunning.get();
    }


    public abstract boolean succeeded();

    public abstract boolean failed();

    public static Optional<Map<String, Object>> getResults(final Task task) {
        final Map<String, Object> results = new HashMap<>() {{
            put(JobResults.Keys.NAME, task.getClass().getSimpleName());
            put(JobResults.Keys.START_TIME, task.getMetadata().get("start"));
            put(JobResults.Keys.CONFIGURATION, task.config);
            put(JobResults.Keys.METADATA, task.getMetadata());
            put(JobResults.Keys.METRICS, task.getMetrics());
        }};
        if (task.succeeded()) {
            return Optional.of(new HashMap<>() {{
                put(JobResults.Keys.STATUS, JobResults.Status.SUCCEEDED);
                put(JobResults.Keys.END_TIME, task.getEndTime());
            }});
        } else if (task.failed()) {
            return Optional.of(new HashMap<>() {{
                put(JobResults.Keys.STATUS, JobResults.Status.FAILED);
                put(JobResults.Keys.FAILURE_CAUSE, task.getFailure());
                put(JobResults.Keys.END_TIME, task.getEndTime());
            }});
        } else {
            return Optional.of(new HashMap<>() {{
                put(JobResults.Keys.STATUS, JobResults.Status.RUNNING);
                put(JobResults.Keys.METADATA, task.getMetadata());
                put(JobResults.Keys.METRICS, task.getMetrics());
            }});
        }
    }

    private long getEndTime() {
        return 0;
    }

    private Map<String, Object> getMetadata() {
        return new HashMap<>() {{
        }};
    }

    private Optional<Exception> getFailure() {
        return Optional.empty();
    }


    public static class JobResults {
        public static enum Status {
            SUCCEEDED,
            FAILED,
            RUNNING
        }

        public static class Keys {
            public static final String STATUS = "status";
            public static final String METADATA = "metadata";
            public static final String METRICS = "metrics";
            public static final String NAME = "name";
            public static final String START_TIME = "start_time";
            public static final String END_TIME = "end_time";
            public static final String CONFIGURATION = "configuration";
            public static final String FAILURE_CAUSE = "failure_cause";
        }
    }
    public abstract List<Runtime.PHASE> getPhases();
}
