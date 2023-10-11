package com.aerospike.movement.runtime.core.local;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.driver.impl.GeneratedOutputIdDriver;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.OneShotSupplier;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.test.mock.encoder.MockEncoder;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.LongStream;

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


    private final Integer TEST_SIZE = 1_000_000 * java.lang.Runtime.getRuntime().availableProcessors();


    @Test
    public void basicRuntimeTest() throws Exception {
        LocalParallelStreamRuntime.closeStatic();
        final Map<String, String> configMap = new HashMap<>() {{
            put(THREADS, String.valueOf(java.lang.Runtime.getRuntime().availableProcessors()));
            put(BATCH_SIZE, String.valueOf(500_000));
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
            put(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());
        }};

        final Configuration config = getMockConfiguration(configMap);
        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);


        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.TWO, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));


        MockUtil.setDefaultMockCallbacks();
        final long msTaken = integrationTest(runtime, config);

//        assert !workChunkDriver.hasNext();
        final double seconds = msTaken / 1000.0;
        final int NUMBER_OF_PHASES = 2;
        System.out.printf("Moved %,d elements in %f seconds at approximately %,d elements per second: \n", TEST_SIZE, seconds, (int) (TEST_SIZE / seconds));
        final int threadCount = Integer.parseInt(LocalParallelStreamRuntime.CONFIG.getOrDefault(THREADS, config));
        assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
        assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT));
    }
}
