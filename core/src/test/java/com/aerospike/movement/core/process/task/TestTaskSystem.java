/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.core.process.task;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.RangedOutputIdDriver;
import com.aerospike.movement.runtime.core.driver.impl.RangedWorkChunkDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.task.MockTask;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Test;

import java.util.*;

import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.THREADS;
import static com.aerospike.movement.util.core.runtime.RuntimeUtil.getAvailableProcessors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTaskSystem extends AbstractMovementTest {
    public static class TestTask extends Task {
        public static class Keys {
            public static final String A = "A";
        }

        public static Task open(Configuration config) {
            return new TestTask(ConfigurationBase.NONE, config);
        }

        protected TestTask(ConfigurationBase configurationMeta, Configuration config) {
            super(configurationMeta, config);
        }

        @Override
        public Configuration getConfig(Configuration config) {
            return ConfigUtil.withOverrides(config, new HashMap<>() {{
                put(Keys.A, selfPair(config.getString(Keys.A)));
            }});
        }

        @Override
        public void init(Configuration config) {

        }

        @Override
        public Map<String, Object> getMetrics() {
            return null;
        }

        @Override
        public boolean succeeded() {
            return false;
        }

        @Override
        public boolean failed() {
            return false;
        }

        @Override
        public List<Runtime.PHASE> getPhases() {
            return null;
        }

        @Override
        public void close() throws Exception {

        }

    }

    public static String selfPair(final String s) {
        return String.format("%s:%s", s, s);
    }

    @Test
    public void testTaskWillSetupConfiguration() {
        Configuration base = new MapConfiguration(new HashMap<>() {{
            put(TestTask.Keys.A, "b");
        }});
        Configuration configured = Task.taskConfig(TestTask.class.getName(), base);
        assertEquals(selfPair("b"), configured.getString(TestTask.Keys.A));
    }


    @Test
    public void testListTasks() {
        RuntimeUtil.openClass(MockTask.class, new MapConfiguration(new HashMap<>()));
        final Map<String, Class<? extends Task>> tasks = RuntimeUtil.getTasks();
        assertTrue(tasks.containsKey(MockTask.class.getSimpleName()));
    }

    @Test
    public void runMockTask() {
        final long TEST_SIZE = 1000;
        final Map<String, Class<? extends Task>> tasks = RuntimeUtil.getTasks();
        RuntimeUtil.openClass(MockTask.class, new MapConfiguration(new HashMap<>()));
        assertTrue(tasks.containsKey(MockTask.class.getSimpleName()));
        final Configuration config = getMockConfiguration(new HashMap<>() {{
            put(THREADS, String.valueOf(getAvailableProcessors()));
            put(BATCH_SIZE, String.valueOf(TEST_SIZE / getAvailableProcessors() / 8));
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, RangedWorkChunkDriver.class.getName());
            put(RangedWorkChunkDriver.Config.Keys.RANGE_BOTTOM,"0");
            put(RangedWorkChunkDriver.Config.Keys.RANGE_TOP,String.valueOf(TEST_SIZE));
            put(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, RangedOutputIdDriver.class.getName());
        }});

        MockUtil.setDefaultMockCallbacks();

        final Runtime runtime = LocalParallelStreamRuntime.open(config);
        MockTask task = (MockTask) RuntimeUtil.openClassRef(MockTask.class.getName(), config);

        final UUID x = (UUID) runtime.runTask(task).next();
        Iterator<Map<String, Object>> statusIterator = RuntimeUtil.statusIteratorForTask(x);
        while (statusIterator.hasNext()) {
            System.out.println(statusIterator.next());
        }
        RuntimeUtil.waitTask(x);
        runtime.close();
    }
}
