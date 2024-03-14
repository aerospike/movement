package com.aerospike.movement.plugin;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.util.core.Pair;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;

import java.util.Iterator;

import static com.aerospike.movement.process.core.Task.getTask;

public class PluginTest extends AbstractMovementTest {
    public static class TestPlugin extends Plugin {
        public Iterator<Object> call(final String taskName, final Configuration config) {
            final Task taskInstance = getTask(taskName, config);
            final Configuration taskConfig = taskInstance.getConfig(config);
            Pair<LocalParallelStreamRuntime, Iterator<Object>> pair = runTask(taskInstance, taskConfig)
                    .orElseThrow(() -> new RuntimeException("Failed to run task: " + taskName));
            Iterator<Object> callIterator = (Iterator<Object>) pair.right;
            return callIterator;
        }

        protected TestPlugin(ConfigurationBase base, Configuration config) {
            super(base, config);
        }

        @Override
        public void init(Configuration config) {

        }

        @Override
        public void plugInto(Object system) {

        }

        @Override
        public void onClose()  {

        }
    }

    @Test
    public void testPlugin() {

    }


}
