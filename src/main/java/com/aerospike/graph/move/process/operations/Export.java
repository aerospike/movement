package com.aerospike.graph.move.process.operations;

import com.aerospike.graph.move.process.Job;
import org.apache.commons.configuration2.Configuration;

import java.util.Map;

public class Export extends Job {
    public Export(Configuration config) {
        super(config);
    }

    @Override
    public Map<String, Object> getMetrics() {
        return null;
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
