package com.aerospike.movement.runtime.core.local;


import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.util.core.ErrorHandler;
import com.aerospike.movement.util.core.Handler;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;

public class RunningPhase implements Iterator<Map<String, Object>> {
    private final ForkJoinTask task;
    private final List<Pipeline> pipelines;
    private final Configuration config;
    public final Runtime.PHASE phase;

    private RunningPhase(final ForkJoinTask task, final List<Pipeline> pipelines, final Runtime.PHASE phase, final Configuration config) {
        this.config = config;
        this.task = task;
        this.pipelines = pipelines;
        this.phase = phase;
    }

    protected static RunningPhase of(final ForkJoinTask task, final List<Pipeline> pipelines, final Runtime.PHASE phase, final Configuration config) {
        return new RunningPhase(task, pipelines, phase, config);
    }

    public List<Output> getOutputs() {
        return pipelines.stream().map(Pipeline::getOutput).collect(Collectors.toList());
    }

    public boolean isDone() {
        return task.isDone();
    }

    public Object get() {
        try {
            return task.get();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(String.format(phase.toString(), e.getMessage()));
        }
    }

    public Iterator<Map<String, Object>> status() {
        return new Iterator<Map<String, Object>>() {
            @Override
            public boolean hasNext() {
                return !isDone();
            }

            @Override
            public Map<String, Object> next() {
                return new PrintableMap(LocalParallelStreamRuntime.outputs.stream()
                        .map(it -> new AbstractMap.SimpleEntry<>(((Loadable) it).getId().toString(), it.getMetrics()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            }
        };

    }

    private static class PrintableMap extends AbstractMap<String, Object> {
        private final Map<String, Object> map;

        public PrintableMap(final Map<String, Object> map) {
            this.map = map;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return null;
        }

        @Override
        public String toString() {
            return map.entrySet().stream().map(it -> String.format("%s: %s", it.getKey(), it.getValue()))
                    .collect(Collectors.joining("\n"));

        }
    }

    public void close() {
        final ErrorHandler errorHandler = RuntimeUtil.loadErrorHandler(this, config);
        pipelines.forEach(p -> RuntimeUtil.closeWrap(p.getOutput(), errorHandler));
        SuppliedWorkChunkDriver.clearSupplierForPhase(this.phase);
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Map<String, Object> next() {
        return null;
    }

    @Override
    public String toString() {
        return phase.toString();
    }
}
