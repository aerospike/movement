package com.aerospike.movement.process.tasks.generator;

import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.process.tasks.generator.Generate;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class TestGenerateTask extends AbstractMovementTest {

    @Test
    public void testGenerateTaskIsRegistered() {
        final Map<String, Class<? extends Task>> tasks = RuntimeUtil.getTasks();
        RuntimeUtil.loadClass(Generate.class.getName());
        assertTrue(tasks.containsKey(Generate.class.getSimpleName()));
    }

}
