/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.runtime.core.local;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.RangedOutputIdDriver;
import com.aerospike.movement.runtime.core.driver.impl.RangedWorkChunkDriver;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.encoder.MockEncoder;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.test.mock.task.MockTask;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE;
import static com.aerospike.movement.config.core.ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.THREADS;
import static com.aerospike.movement.test.mock.MockUtil.getHitCounter;
import static junit.framework.TestCase.assertEquals;

public class TestLocalParallelStreamRuntime extends AbstractMovementTest {
    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    private final Integer TEN_MILLION = 10_000_000;
    private final Integer MILLION = 1_000_000;
    private final Integer THOUSAND = 1_000;
    private final Integer HUNDRED_THOUSAND = 100_000;

    @Test
    public void basicRuntimeTest() throws Exception {
        final Integer TEST_SIZE = TEN_MILLION;

        LocalParallelStreamRuntime.closeStatic();
        final Map<String, String> configMap = new HashMap<>() {{
            put(THREADS, String.valueOf(java.lang.Runtime.getRuntime().availableProcessors()));
            put(BATCH_SIZE, String.valueOf(1000));
            put(WORK_CHUNK_DRIVER_PHASE_ONE, RangedWorkChunkDriver.class.getName());
            put(WORK_CHUNK_DRIVER_PHASE_TWO, RangedWorkChunkDriver.class.getName());
            put(RangedWorkChunkDriver.Config.Keys.RANGE_BOTTOM, "0");
            put(RangedWorkChunkDriver.Config.Keys.RANGE_TOP, String.valueOf(TEST_SIZE));
            put(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, RangedOutputIdDriver.class.getName());
        }};

        final Configuration config = getMockConfiguration(configMap);
        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);


        MockUtil.setDefaultMockCallbacks();
        final long msTaken = iteratePhasesTimed(runtime, config);

        final double seconds = msTaken / 1000.0;
        final int NUMBER_OF_PHASES = 2;
        RuntimeUtil.getLogger(TestLocalParallelStreamRuntime.class.getSimpleName()).info("Moved %,d elements in %f seconds at approximately %,d elements per second: \n", TEST_SIZE, seconds, (int) (TEST_SIZE / seconds));
        runtime.close();
        final int threadCount = Integer.parseInt(LocalParallelStreamRuntime.CONFIG.getOrDefault(THREADS, config));
//        assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
//        assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT));
    }

    @Test
    @Ignore //@todo
    public void concurrencyControlBasicTest() throws Exception {
        final Integer TEST_SIZE = TEN_MILLION;
        LocalParallelStreamRuntime.closeStatic();
        final Map<String, String> configMap = new HashMap<>() {{
            put(THREADS, "1");
            put(BATCH_SIZE, String.valueOf(13));
            put(WORK_CHUNK_DRIVER_PHASE_ONE, RangedWorkChunkDriver.class.getName());
            put(WORK_CHUNK_DRIVER_PHASE_TWO, RangedWorkChunkDriver.class.getName());

            put(RangedWorkChunkDriver.Config.Keys.RANGE_BOTTOM, "0");
            put(RangedWorkChunkDriver.Config.Keys.RANGE_TOP, String.valueOf(TEST_SIZE));
            put(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, RangedOutputIdDriver.class.getName());
        }};
        final Configuration config = getMockConfiguration(configMap);
        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);


        MockUtil.setDefaultMockCallbacks();

        final int threadCount = Integer.parseInt(LocalParallelStreamRuntime.CONFIG.getOrDefault(THREADS, config));
        assertEquals(1, threadCount);

        Iterator<RunningPhase> phaseIterator = runtime.runPhases(List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO), config);
        RunningPhase phaseOne = phaseIterator.next();
        phaseOne.get();
        phaseOne.close();
        final int maxConcurrencyPhaseOne = phaseOne.processor.maxRunningTasks.get();
        assertEquals(threadCount, maxConcurrencyPhaseOne);
        RunningPhase phaseTwo = phaseIterator.next();
        phaseTwo.get();
        phaseTwo.close();
        final int maxConcurrencyPhaseTwo = phaseTwo.processor.maxRunningTasks.get();
        assertEquals(threadCount, maxConcurrencyPhaseTwo);

        final int NUMBER_OF_PHASES = 2;

        runtime.close();

        assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
        assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT));
    }

    @Test
    @Ignore
    public void testMockTaskConcurrency() throws Exception {
        final Integer TEST_SIZE = TEN_MILLION;

        final Configuration config = getMockConfiguration(new HashMap<>() {{
            put(THREADS, "16");
            put(BATCH_SIZE, String.valueOf(10));
            put(WORK_CHUNK_DRIVER_PHASE_ONE, RangedWorkChunkDriver.class.getName());
            put(WORK_CHUNK_DRIVER_PHASE_TWO, RangedWorkChunkDriver.class.getName());

            put(RangedWorkChunkDriver.Config.Keys.RANGE_BOTTOM, "0");
            put(RangedWorkChunkDriver.Config.Keys.RANGE_TOP, String.valueOf(1 + TEST_SIZE));
            put(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, RangedOutputIdDriver.class.getName());
        }});
        MockUtil.setDefaultMockCallbacks();

        RuntimeUtil.loadClass(MockTask.class.getName());
        testTaskConcurrencyControl(MockTask.class.getSimpleName(), config);
    }

    public static void testTaskConcurrencyControl(final String taskName, final Configuration config) throws Exception {
        LocalParallelStreamRuntime.closeStatic();
        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);

        final int threadCount = Integer.parseInt(LocalParallelStreamRuntime.CONFIG.getOrDefault(THREADS, config));
//        assertEquals(1, threadCount);


        final Iterator<RunningPhase> phaseIterator = runtime.runPhases(List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO), config);
        final RunningPhase phaseOne = phaseIterator.next();
        phaseOne.get();
        phaseOne.close();
        final int maxConcurrencyPhaseOne = phaseOne.processor.maxRunningTasks.get();
        RuntimeUtil.getLogger().info("maxConcurrencyPhaseOne:" + maxConcurrencyPhaseOne);
//        assertEquals(threadCount, maxConcurrencyPhaseOne);

        final RunningPhase phaseTwo = phaseIterator.next();
        phaseTwo.get();
        phaseTwo.close();
        final int maxConcurrencyPhaseTwo = phaseTwo.processor.maxRunningTasks.get();
        RuntimeUtil.getLogger().info("maxConcurrencyPhaseTwo:" + maxConcurrencyPhaseTwo);

//        assertEquals(threadCount, maxConcurrencyPhaseTwo);
    }

}
