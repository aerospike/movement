package com.aerospike.movement.emitter.tinkerpop;

import com.aerospike.movement.test.core.AbstractMovementTest;
import org.apache.commons.configuration2.Configuration;
import org.junit.After;
import org.junit.Before;

import java.util.*;

import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.THREADS;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class TestTinkerPopEmitter extends AbstractMovementTest {
    final int THREAD_COUNT = 4;
    final int TEST_SIZE = 20;
    final Map<String, String> configMap = new HashMap<>() {{
        put(THREADS, String.valueOf(THREAD_COUNT));
    }};
    final Configuration mockConfig = getMockConfiguration(configMap);

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

}
