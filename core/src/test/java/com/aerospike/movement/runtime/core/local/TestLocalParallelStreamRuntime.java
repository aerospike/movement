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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.stream.IntStream;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE;
import static com.aerospike.movement.config.core.ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.THREADS;
import static com.aerospike.movement.test.mock.MockUtil.getHitCounter;
import static junit.framework.TestCase.assertEquals;

@RunWith(Parameterized.class)
public class TestLocalParallelStreamRuntime extends AbstractMovementTest {
    final static Integer testLoops = 20;
    private final Integer threadCount;
    private final Integer batchSize;

    @Parameterized.Parameters
    public static Collection<Integer[]> data() {
        final List<Integer[]> testParams = new ArrayList<>();
        IntStream.range(0, testLoops).forEach(loop -> {
            testParams.add(new Integer[]{
                    (1 + new Random().nextInt(RuntimeUtil.getAvailableProcessors() * 2)),
                    (1 + new Random().nextInt(2000))

            });
        });
        return testParams;
    }

    public TestLocalParallelStreamRuntime(final Integer threadCount, Integer batchSize) {
        this.batchSize = batchSize;
        this.threadCount = threadCount;
    }

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
    @Ignore
    public void basicRuntimeTest() throws Exception {
        final Integer TEST_SIZE = MILLION * threadCount;

        LocalParallelStreamRuntime.closeStatic();
        final Map<String, String> configMap = new HashMap<>() {{
            put(THREADS, String.valueOf(threadCount));
            put(BATCH_SIZE, String.valueOf(batchSize));
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
        RuntimeUtil.getLogger(TestLocalParallelStreamRuntime.class.getSimpleName()).info("Moved %,d elements in %f seconds at approximately %,d elements per second", TEST_SIZE, seconds, (int) (TEST_SIZE / seconds));
        runtime.close();
        final int threadCount = Integer.parseInt(LocalParallelStreamRuntime.CONFIG.getOrDefault(THREADS, config));
        assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
        assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT));
    }
}
