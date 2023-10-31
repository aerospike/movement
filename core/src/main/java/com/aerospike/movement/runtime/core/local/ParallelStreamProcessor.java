/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.local;

import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.Handler;
import com.aerospike.movement.runtime.core.Pipeline;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;

import com.aerospike.movement.util.core.coordonation.WaitGroup;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelStreamProcessor implements Runnable {
    private final List<Pipeline> pipelines;
    private final Configuration config;
    private final Runtime.PHASE phase;
    private final ErrorHandler errorHandler;
    public final LocalParallelStreamRuntime runtime;
    public final AtomicInteger runningTasks = new AtomicInteger(0);
    public final AtomicInteger maxRunningTasks = new AtomicInteger(0);

    private ParallelStreamProcessor(final List<Pipeline> pipelines, final Runtime.PHASE phase, final LocalParallelStreamRuntime runtime, final Configuration config) {
        this.pipelines = pipelines;
        this.config = config;
        this.phase = phase;
        this.runtime = runtime;
        this.errorHandler = RuntimeUtil.getErrorHandler(this, config);
    }

    public static ParallelStreamProcessor create(final List<Pipeline> pipelines, final Configuration config, final LocalParallelStreamRuntime runtime, final Runtime.PHASE phase) {
        return new ParallelStreamProcessor(pipelines, phase, runtime, config);
    }

    private static class RuntimeErrorHandler implements Handler<Throwable> {
        private final WaitGroup waitGroup;
        private final ErrorHandler upstream;

        private RuntimeErrorHandler(final WaitGroup waitGroup, final ErrorHandler upstream) {
            this.waitGroup = waitGroup;
            this.upstream = upstream;
        }

        public static RuntimeErrorHandler create(final WaitGroup waitGroup, final ErrorHandler upstream) {
            return new RuntimeErrorHandler(waitGroup, upstream);
        }

        @Override
        public void handle(final Throwable t, final Object... context) {
            waitGroup.done();
            upstream.handleError(t, waitGroup);
        }
    }

    //Make your elementIterators size equal to your threadpool size
    @Override
    public void run() {
        final WaitGroup waitGroup = WaitGroup.of(pipelines.size());
        pipelines.stream().parallel().forEach(pipeline -> {
            maxRunningTasks.getAndUpdate(existingMax -> {
                final int currentTaskCount = runningTasks.incrementAndGet();
                return Math.max(existingMax, currentTaskCount);
            });
            final Emitter emitter = pipeline.getEmitter();
            final Output output = pipeline.getOutput();
            final WorkChunkDriver driver = (WorkChunkDriver) RuntimeUtil.lookupOrLoad(WorkChunkDriver.class, config);
            final ErrorHandler errorHandler = RuntimeUtil.getErrorHandler(this, config);
            try {
                RuntimeUtil.driveIndividualThreadSync(phase,
                        driver,
                        emitter,
                        output,
                        () -> {
                            RuntimeUtil.closeWrap(pipeline);
                            waitGroup.done();
                        }, RuntimeErrorHandler.create(waitGroup, errorHandler)
                );
            } catch (Exception e) {
                runningTasks.decrementAndGet();
                throw errorHandler.handleError(e, pipeline);
            }
            runningTasks.decrementAndGet();
        });
        try {
            waitGroup.await();
            RuntimeUtil.closeAllInstancesOfLoadable(WorkChunkDriver.class);
            RuntimeUtil.unload(WorkChunkDriver.class);
        } catch (InterruptedException e) {
            throw errorHandler.handleError(e, phase, pipelines, waitGroup);
        }
    }
}
