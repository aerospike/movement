/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.local;


import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.Pipeline;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;

import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.runtime.IOUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.reflections.serializers.JsonSerializer;

import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.PHASE;
import static com.aerospike.movement.runtime.core.local.RunningPhase.Keys.PIPELINES;
import static com.aerospike.movement.runtime.core.local.RunningPhase.Keys.PIPELINE_COUNT;

public class RunningPhase implements Iterator<Map<String, Object>> {
    public final ParallelStreamProcessor processor;

    public static class Keys {
        public static final String OUTPUTS = "outputs";
        public static final String PIPELINES = "pipelines";
        public static final String PIPELINE_COUNT = "pipelineCount";


    }

    private final ForkJoinTask task;
    private final List<Pipeline> pipelines;
    private final Configuration config;
    public final Runtime.PHASE phase;

    private RunningPhase(final ParallelStreamProcessor processor, final ForkJoinTask task, final List<Pipeline> pipelines, final Runtime.PHASE phase, final Configuration config) {
        this.config = config;
        this.processor = processor;
        this.task = task;
        this.pipelines = pipelines;
        this.phase = phase;
    }

    protected static RunningPhase execute(final ParallelStreamProcessor processor, final List<Pipeline> pipelines, final Runtime.PHASE phase, final Configuration config) {
        final ForkJoinTask task = processor.runtime.customThreadPool.submit(processor);
        return new RunningPhase(processor, task, pipelines, phase, config);
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
            throw RuntimeUtil.getErrorHandler(this).handleError(new RuntimeException(String.format(phase.toString(), e.getMessage())));
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
                RuntimeUtil.stall(100L);
                final Map<String, Object> status = new HashMap<>();
                pipelines.stream().map(it -> it.getOutput()).forEach(output -> {
                    status.put(output.getClass().getSimpleName() + ":" + ((Loadable) output).getId().toString().split("-")[0], output.getMetrics());
                });


                return new HashMap<>() {{
                    put(Keys.OUTPUTS, status);
                }};
            }
        };

    }


    public void close() {
        final ErrorHandler errorHandler = RuntimeUtil.getErrorHandler(this, config);
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
        return IOUtil.formatStruct(new HashMap<>() {{
            put(PHASE, phase.name());
            put(PIPELINES, pipelines.stream().map(it -> it.toString()).collect(Collectors.toList()));
            put(PIPELINE_COUNT, pipelines.size());
        }});
    }
}
