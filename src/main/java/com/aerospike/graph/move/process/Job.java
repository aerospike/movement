package com.aerospike.graph.move.process;

import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.process.operations.Export;
import com.aerospike.graph.move.process.operations.Generate;
import com.aerospike.graph.move.process.operations.Load;
import com.aerospike.graph.move.process.operations.Migrate;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Job {
    private final String id;
    private final Configuration config;

    final AtomicBoolean isRunning = new AtomicBoolean(false);


    private static final Map<String, Job> jobs = new ConcurrentHashMap<>();

    public enum JobType {
        GENERATE(Generate.class.getName()),
        LOAD(Load.class.getName()),
        MIGRATE(Migrate.class.getName()),
        EXPORT(Export.class.getName());


        private final String value;

        JobType(final String value) {
            this.value = value;
        }

        public final String getValue() {
            return value;
        }

        public final Class<? extends Job> getClass(Configuration config) {
            return RuntimeUtil.loadClass(this.getValue());
        }
    }

    protected Job(Configuration config) {
        this.id = UUID.randomUUID().toString();
        long startTime = System.currentTimeMillis();
        this.config = config;
    }

    public static Job createJob(final JobType type, final Configuration config) {
        try {
            return type.getClass(config)
                    .getConstructor(Runtime.class, Configuration.class)
                    .newInstance(Runtime.getLocalRuntime(config), config);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    //Run blocking, return metadata from completed or failed run.
    public Map<String, Object> runSync() {
        return null;
    }


    public abstract Map<String, Object> getMetrics();

    public abstract boolean isRunning();

    public abstract boolean succeeded();

    public abstract boolean failed();

    public static Optional<Map<String, Object>> getResults(final Job job) {
        if (job.succeeded()) {
            return Optional.of(new HashMap<>() {{
                put("metadata", job.getMetadata());
                put("metrics", job.getMetrics());
                put("status", "succeeded");
            }});
        } else if (job.failed()) {
            return Optional.of(new HashMap<>() {{
                put("metadata", job.getMetadata());
                put("metrics", job.getMetrics());
                put("cause", job.getFailure());
                put("status", "failed");
            }});
        } else { //still running
            return Optional.empty();
        }
    }

    private Map<String, Object> getMetadata() {
        return new HashMap<>() {{
            put("type", this.getClass().getName());
            put("start", System.currentTimeMillis());
        }};
    }

    private Optional<Exception> getFailure() {
        return Optional.empty();
    }

}
