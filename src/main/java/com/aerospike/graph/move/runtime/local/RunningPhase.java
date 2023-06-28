package com.aerospike.graph.move.runtime.local;

import com.aerospike.graph.move.output.Output;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;

public class RunningPhase {
    private final ForkJoinTask task;
    private final List<Output> outputs;

    public RunningPhase(final Map.Entry<ForkJoinTask, List<Output>> entry) {
        this.task = entry.getKey();
        this.outputs = List.copyOf(entry.getValue());
    }

    public static RunningPhase of(Map.Entry<ForkJoinTask, List<Output>> entry) {
        return new RunningPhase(entry);
    }

    public List<Output> getOutputs() {
        return outputs;
    }

    public Object get() {
        try {
            return task.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void close() {
        outputs.forEach(Output::close);
    }
}
