/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.local;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.Handler;
import com.aerospike.movement.runtime.core.Pipeline;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;

import com.aerospike.movement.util.core.coordonation.WaitGroup;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.aerospike.movement.util.core.runtime.RuntimeUtil.getAvailableProcessors;

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

    @Override
    public void run() {
        final WaitGroup waitGroup = WaitGroup.of(pipelines.size());
        final ExecutorService executorService = Executors.newFixedThreadPool(pipelines.size() + 1);
        pipelines.forEach(pipeline -> {
            executorService.submit(() -> {
                maxRunningTasks.getAndUpdate(existingMax -> {
                    final int currentTaskCount = runningTasks.incrementAndGet();
                    return Math.max(existingMax, currentTaskCount);
                });
                final Emitter emitter = pipeline.getEmitter();
                final Output output = pipeline.getOutput();
                final WorkChunkDriver driver = (WorkChunkDriver) RuntimeUtil.lookupOrLoad(WorkChunkDriver.class, config);
                final ErrorHandler errorHandler = RuntimeUtil.getErrorHandler(this, config);
                try {
                    driveIndividualThreadSync(phase,
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
                    throw errorHandler.handleFatalError(e, pipeline);
                }
                runningTasks.decrementAndGet();
            });
        });
        try {
            waitGroup.await();
            executorService.shutdown();
            RuntimeUtil.closeAllInstancesOfLoadable(WorkChunkDriver.class);
            RuntimeUtil.closeAllInstancesOfLoadable(Emitter.class);
            RuntimeUtil.closeAllInstancesOfLoadable(Encoder.class);
            RuntimeUtil.closeAllInstancesOfLoadable(Output.class);
            RuntimeUtil.unload(WorkChunkDriver.class);
        } catch (InterruptedException e) {
            throw errorHandler.handleError(e, phase, pipelines, waitGroup);
        }
    }

    public static void driveIndividualThreadSync(final Runtime.PHASE phase,
                                                 final WorkChunkDriver driver,
                                                 final Emitter emitter,
                                                 final Output output,
                                                 final Runnable completionHandler,
                                                 final Handler<Throwable> errorHandler) {
        try {
            final Iterator<Emitable> emitableIterator = emitter.stream(driver, phase).iterator();
            while (emitableIterator.hasNext()) {
                processEmitable(emitableIterator.next(), output);
            }
        } catch (Exception e) {
            errorHandler.handle(RuntimeUtil.getErrorHandler(LocalParallelStreamRuntime.class).handleFatalError(e, output), output);
        }
        completionHandler.run();
    }

    public static void processEmitable(final Emitable emitable, final Output output) {
        IteratorUtils.iterate(walk(emitable.emit(output), output));
    }

    public static Iterator<Emitable> walk(final Stream<Emitable> input, final Output output) {
        return IteratorUtils.flatMap(input.iterator(), emitable -> {
            try {
                return walk(emitable.emit(output), output);
            } catch (final Exception e) {
                throw RuntimeUtil.getErrorHandler(output, new MapConfiguration(new HashMap<>())).handleFatalError(e, output);
            }
        });
    }
}
