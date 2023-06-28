package com.aerospike.graph.move.process.operations;

import com.aerospike.graph.move.process.Job;
import com.aerospike.graph.move.util.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Map;

public class Load extends Job {
    public Load(Configuration config) {
        super(config);
    }

    @Override
    public Map<String, Object> getMetrics() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean succeeded() {
        return false;
    }

    @Override
    public boolean failed() {
        return false;
    }
}