package com.aerospike.movement.process.tasks.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.test.mock.task.MockTask;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.List;
import java.util.Map;

public class TaskStatus extends Task {
    static {
        RuntimeUtil.registerTaskAlias(TaskStatus.class.getSimpleName(), TaskStatus.class);
    }

    public static TaskStatus open(Configuration configuration){
        return new TaskStatus(configuration);
    }
    protected TaskStatus(Configuration config) {
        super(ConfigurationBase.NONE,config);
    }

    @Override
    public Configuration getConfig(Configuration config) {
        return this.config;
    }

    @Override
    public Map<String, Object> getMetrics() {
        return null;
    }

    @Override
    public boolean succeeded() {
        return false;
    }

    @Override
    public boolean failed() {
        return false;
    }

    @Override
    public List<Runtime.PHASE> getPhases() {
        return List.of();
    }

    @Override
    public void init(Configuration config) {

    }

    @Override
    public void close() throws Exception {

    }
}
