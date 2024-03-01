/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.test.core;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.emitter.MockEmitter;
import com.aerospike.movement.test.mock.encoder.MockEncoder;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.*;

public abstract class AbstractMovementTest {
    final List<Runnable> cleanupCallbacks = new ArrayList<>();
    static Map<String, String> mockConfig = new HashMap<>() {{
        put(ConfigurationBase.Keys.EMITTER, MockEmitter.class.getName());
        put(ConfigurationBase.Keys.OUTPUT, MockOutput.class.getName());
        put(ConfigurationBase.Keys.ENCODER, MockEncoder.class.getName());
    }};

    public static Configuration getMockConfiguration(final Map<String, String> config) {
        config.putAll(getMockConfigurationMap());
        return new MapConfiguration(config);
    }

    public static Map<String, String> getMockConfigurationMap() {
        return mockConfig;
    }
    public void registerCleanupCallback(Runnable callback) {
        this.cleanupCallbacks.add(callback);
    }

    public void setup() {
        clearMock();
    }

    public void cleanup() {
        this.cleanupCallbacks.forEach(Runnable::run);

        clearMock();
    }

    public static long iteratePhasesTimed(final Runtime runtime, final Configuration config) {
        return iteratePhasesTimed(runtime, List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO), config);
    }

    public static long iteratePhasesTimed(final Runtime runtime, final List<Runtime.PHASE> phases, final Configuration config) {
        final long now = System.currentTimeMillis();
        final Iterator<RunningPhase> phaseIterator = runtime.runPhases(phases, config);
        iteratePhasesAndCloseRuntime(phaseIterator, runtime);
        final long timeTaken = System.currentTimeMillis() - now;
        return timeTaken;
    }

    public static void iteratePhasesAndCloseRuntime(final Iterator<RunningPhase> phaseIterator, final Runtime runtime) {
        while (phaseIterator.hasNext()) {
            final RunningPhase phase = phaseIterator.next();
            phase.get();
            phase.close();
            runtime.close();
        }
        runtime.close();
    }

    public void clearMock() {
        MockUtil.clear();
    }
    public static void testTask(Class<? extends Task> taskClass, Configuration config) {
        final Runtime runtime = LocalParallelStreamRuntime.open(config);
        Task task = (Task) RuntimeUtil.openClass(taskClass, config);
        final UUID x = (UUID) runtime.runTask(task).next();
        Iterator<Map<String, Object>> statusIterator = RuntimeUtil.statusIteratorForTask(x);
        while (statusIterator.hasNext()) {
            RuntimeUtil.getLogger().info(statusIterator.next());
        }
        RuntimeUtil.waitTask(x);
    }
}
