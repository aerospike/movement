/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.process.core;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.util.core.runtime.IOUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

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

//    public abstract Configuration setupConfig(final Configuration inputConfig);

    public static Task getTaskByAlias(final String operation, final Configuration config) {
        Class<? extends Task> task = RuntimeUtil.getTaskClassByAlias(operation);
        return (Task) RuntimeUtil.lookupOrLoad(task, config);
    }

    public abstract Configuration getConfig(Configuration config);

    public static Iterator<RunningPhase> run(final Task task) {
        final Configuration jobConfig = task.getConfig(task.config);
        final LocalParallelStreamRuntime runtime = (LocalParallelStreamRuntime) LocalParallelStreamRuntime.getInstance(jobConfig);
        final List<Runtime.PHASE> phaseList = task.getPhases();
        return phaseList.stream()
                .map(it -> {
                    RuntimeUtil.getLogger(task).info("running phase: " + it);
                    return runtime.runPhase(it, jobConfig);
                }).iterator();
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
        public enum Status {
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


    public static Configuration taskConfig(final String taskImplClassName, final Configuration config) {
        final Task taskInstance = (Task) RuntimeUtil.openClassRef(taskImplClassName, config);
        return taskInstance.getConfig(taskInstance.config);
    }

    public static Task getTask(final String taskName, final Configuration config) {
        final String taskImplClassName = RuntimeUtil.getTaskClassByAlias(taskName).getName();
        return (Task) RuntimeUtil.openClassRef(taskImplClassName, config);
    }

    public static class StatusMonitor {
        private final UUID taskId;
        private final Iterator<Map<String, Object>> statusIterator;

        public StatusMonitor(UUID taskId) {
            this.taskId = taskId;
            this.statusIterator = RuntimeUtil.statusIteratorForTask(taskId);
        }

        public static StatusMonitor from(UUID taskId) {
            return new StatusMonitor(taskId);
        }

        public boolean isRunning() {
            return RuntimeUtil.statusIteratorForTask(taskId).hasNext();
        }

        private long lastCheck = 0L;
        private final long startTime = System.nanoTime();
        private Map<UUID, Long> oldIOCounters = new HashMap<>();

        public String statusMessage() {
            return statusMessage(false);
        }

        public String statusMessage(final boolean debug) {
            return IOUtil.formatStruct(status(debug));
        }

        public Map<String, Object> status(final boolean debug) {
            Iterator<Map<String, Object>> statusIterator = RuntimeUtil.statusIteratorForTask(taskId);
            Optional<RunningPhase> potentialRunningPhase = RuntimeUtil.runningPhaseForTask(taskId);
            Optional<Map<String, Object>> nextStatusOption = statusIterator.hasNext() ? Optional.of(statusIterator.next()) : Optional.empty();
            final String state = nextStatusOption.isPresent() ? "RUNNING" : "STOPPED";
            final long now = System.nanoTime();
            final long timeSinceLastCheckNs = now - lastCheck;
            final long timeSinceStartNs = now - startTime;

            final double timeSinceLastCheckSec = TimeUnit.of(ChronoUnit.SECONDS).convert(Duration.ofNanos(timeSinceLastCheckNs));
            final double timeSinceLastStartSec = TimeUnit.of(ChronoUnit.SECONDS).convert(Duration.ofNanos(timeSinceStartNs));
            lastCheck = now;
            Map<String, Object> statusMessageData = new HashMap<>() {{
                put("STATE", state);
            }};
            class Keys {
                public static final String TOTAL = "total";
                public static final String DELTA = "delta";
                public static final String ELAPSED = "elapsed";
                public static final String AVERAGE = "average";
            }

            final String formatString = "issued %d total io operations, %d in the last %.2f seconds with an overall average of %.2f/sec";
            Function<Map<String, Number>, String> formatter = (Map<String, Number> data) -> {
                return String.format(formatString, data.get(Keys.TOTAL), data.get(Keys.DELTA), data.get(Keys.ELAPSED), data.get(Keys.AVERAGE));
            };
            if (nextStatusOption.isPresent()) {
                List<Output> outputs = potentialRunningPhase.get().getOutputs();

                List<Map.Entry<String, Map<String, Number>>> outputData = outputs.stream().map(output -> {
                    final UUID outputId = ((Loadable) (output)).getId();
                    final long ioOps = (Long) output.getMetrics().getOrDefault("io_ops",0l);
                    String outputkey = output.getClass().getSimpleName() + outputId.toString().split("-")[0];
                    final Map<String, Number> vals = Map.of(
                            Keys.TOTAL, ioOps,
                            Keys.DELTA, ioOps - oldIOCounters.computeIfAbsent(outputId, (uuid) -> 0L),
                            Keys.ELAPSED, timeSinceLastCheckSec,
                            Keys.AVERAGE, ioOps / timeSinceLastStartSec
                    );
                    oldIOCounters.put(outputId, ioOps);
                    return Map.entry(outputkey, vals);
                }).collect(Collectors.toList());
                long workChunks = WorkChunkDriver.metric.get();
                statusMessageData.put("PERFORMANCE", new HashMap<>() {{
                    put("TOTAL_IO", outputData.stream().map(ev -> (Long) ev.getValue().get(Keys.TOTAL)).reduce((a, b) -> a + b).orElse(0L));
                    put("INTERVAL_IO", outputData.stream().map(ev -> (Long) ev.getValue().get(Keys.DELTA)).reduce((a, b) -> a + b).orElse(0L));
                    put("INTERVAL_UNIT", "SECOND");
                    put("INTERVAL_TIME", timeSinceLastCheckSec);
                    put("AVERAGE_IO", outputData.stream().map(ev -> (Double) ev.getValue().get(Keys.AVERAGE)).reduce((a, b) -> a + b).orElse(0.0));
                }});
                statusMessageData.put("WORK_CHUNKS", workChunks);
                statusMessageData.put("OUTPUTS", outputData.stream().map(it -> Map.entry(it.getKey(), formatter.apply(it.getValue()))).collect(Collectors.toList()));
                if (debug)
                    potentialRunningPhase.ifPresent(runningPhase -> statusMessageData.put("RUNNING_PHASE", potentialRunningPhase.map(thing -> thing.status().next()).orElse(Map.of())));
            }
            return statusMessageData;
        }
    }
}
