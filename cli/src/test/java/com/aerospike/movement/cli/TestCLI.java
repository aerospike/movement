package com.aerospike.movement.cli;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.generator.schema.YAMLParser;
import com.aerospike.movement.plugin.cli.CLIPlugin;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.process.tasks.generator.Generate;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.runtime.core.driver.impl.GeneratedOutputIdDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.iterator.OneShotSupplier;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.encoder.MockEncoder;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.test.mock.task.MockTask;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.aerospike.movement.emitter.generator.Generator.Config.Keys.SCALE_FACTOR;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.THREADS;
import static com.aerospike.movement.test.mock.MockUtil.getHitCounter;
import static com.aerospike.movement.util.core.RuntimeUtil.getAvailableProcessors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class TestCLI extends AbstractMovementTest {
    @Test
    public void testHelp() throws Exception {
        String[] args = {CLI.MovementCLI.Args.HELP_LONG};
        assertTrue(CLI.parseAndLoadPlugin(args).isEmpty());
    }

    @Before
    public void setup() {
        LocalParallelStreamRuntime.closeStatic();
        MockUtil.clear();
    }

    @After
    public void cleanup() {
        LocalParallelStreamRuntime.closeStatic();
        MockUtil.clear();
    }

    @Test
    public void testListTasks() throws Exception {
        RuntimeUtil.loadClass(MockTask.class.getName());
        final Map<String, Class<? extends Task>> tasks = RuntimeUtil.getTasks();
        assertTrue(tasks.containsKey(MockTask.class.getSimpleName()));

        String[] args = {CLI.MovementCLI.Args.TEST_MODE,
                CLI.MovementCLI.Args.LIST_TASKS};
        final Optional<CLIPlugin> x = CLI.parseAndLoadPlugin(args);
        assertTrue(x.isPresent());
        final List<Object> y = IteratorUtils.list(x.get().call());
        final List<String> z = RuntimeUtil.findAvailableSubclasses(Task.class)
                .stream().map(Class::getName).collect(Collectors.toList());
        assertEquals(2, z.size());
        assertEquals(z.size(), y.size());
        CLI.main(args);
    }

    @Test
    public void testListComponents() throws Exception {
        RuntimeUtil.loadClass(MockTask.class.getName());
        final Map<String, Class<? extends Task>> tasks = RuntimeUtil.getTasks();
        assertTrue(tasks.containsKey(MockTask.class.getSimpleName()));

        final String[] args = {CLI.MovementCLI.Args.TEST_MODE,CLI.MovementCLI.Args.LIST_COMPONENTS};
        final Optional<CLIPlugin> x = CLI.parseAndLoadPlugin(args);
        assertTrue(x.isPresent());
        final List<Object> y = IteratorUtils.list(x.get().call());
        for (Object s : y) {
            System.out.println(s);
        }
        assertTrue(y.size() > 3);
        CLI.main(args);
    }

    private String override(final String key, final String value) {
        return String.format("%s=%s", key, value);
    }

    private final Integer TEST_SIZE = 10_000;


    @Test
    public void testRunGenerateTask() throws Exception {
        assertEquals(0, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
        RuntimeUtil.loadClass(MockTask.class.getName());
        final Path tmpConfig = Files.createTempFile("movement", "test").toAbsolutePath();
        final String configString = ConfigurationUtil.configurationToPropertiesFormat(getMockConfiguration(new HashMap<>()));
        Files.write(tmpConfig, configString.getBytes());

        final String[] args = {
                CLI.MovementCLI.Args.TEST_MODE,
                CLI.MovementCLI.Args.TASK, Generate.class.getSimpleName(),
                CLI.MovementCLI.Args.CONFIG_SHORT, tmpConfig.toString(),
                CLI.MovementCLI.Args.SET_SHORT, override(THREADS, "1"),
                CLI.MovementCLI.Args.SET_SHORT, override(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName()),
                CLI.MovementCLI.Args.SET_SHORT, override(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName()),
                CLI.MovementCLI.Args.SET_SHORT, override(SCALE_FACTOR, TEST_SIZE.toString()),
        };
        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));
        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.TWO, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));

        MockUtil.setDefaultMockCallbacks();


        final Optional<CLIPlugin> x = CLI.parseAndLoadPlugin(args);
        assertTrue(x.isPresent());
        final Iterator<Object> iterator = x.get().call();
        while (iterator.hasNext()) {
            final Object it = iterator.next();
            if (!iterator.hasNext()) {
                System.out.println(it);
            }
        }

        final int NUMBER_OF_PHASES = 1;
        TestCase.assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
        TestCase.assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT));
    }


    @Test
    public void testRunMockTask() throws Exception {
        assertEquals(0, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
        RuntimeUtil.loadClass(MockTask.class.getName());
        final Path tmpConfig = Files.createTempFile("movement", "test").toAbsolutePath();
        final String configString = ConfigurationUtil.configurationToPropertiesFormat(getMockConfiguration(new HashMap<>()));
        Files.write(tmpConfig, configString.getBytes());


        final String[] args = {
                CLI.MovementCLI.Args.TEST_MODE,
                CLI.MovementCLI.Args.TASK, MockTask.class.getSimpleName(),
                CLI.MovementCLI.Args.CONFIG_SHORT, tmpConfig.toString(),
                CLI.MovementCLI.Args.DEBUG_SHORT,
                CLI.MovementCLI.Args.SET_SHORT, override(THREADS, "1"),
                CLI.MovementCLI.Args.SET_SHORT, override(BATCH_SIZE, String.valueOf(TEST_SIZE / getAvailableProcessors() / 8)),
                CLI.MovementCLI.Args.SET_SHORT, override(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName()),
                CLI.MovementCLI.Args.SET_SHORT, override(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName()),
        };
        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));
        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.TWO, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));

        MockUtil.setDefaultMockCallbacks();

        final Optional<CLIPlugin> x = CLI.parseAndLoadPlugin(args);
        assertTrue(x.isPresent());
        final Iterator<Object> iterator = x.get().call();
        while (iterator.hasNext()) {
            final Object it = iterator.next();
            if (!iterator.hasNext()) {
                System.out.println(it);
            }
        }
        LocalParallelStreamRuntime.closeStatic();

        final int NUMBER_OF_PHASES = 1;
        TestCase.assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
        TestCase.assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT));
    }

    @Test
    public void testRunMockTaskMain() throws Exception {
        long TEST_SIZE = 40000;
        RuntimeUtil.loadClass(MockTask.class.getName());
        final Path exampleConfig = Path.of("../conf/generator/example.properties");

        final String[] args = {
                CLI.MovementCLI.Args.TEST_MODE,

                CLI.MovementCLI.Args.TASK, MockTask.class.getSimpleName(),
                CLI.MovementCLI.Args.CONFIG_SHORT, exampleConfig.toString(),
                CLI.MovementCLI.Args.DEBUG_SHORT,

                CLI.MovementCLI.Args.SET_SHORT, override(THREADS, "8"),
                CLI.MovementCLI.Args.SET_SHORT, override(SCALE_FACTOR, String.valueOf(TEST_SIZE)),
                CLI.MovementCLI.Args.SET_SHORT, override(YAMLParser.Config.Keys.YAML_FILE_PATH, "../extensions/generator/src/main/resources/example_schema.yaml"),
                CLI.MovementCLI.Args.SET_SHORT, override(BATCH_SIZE, String.valueOf(10000)),
                CLI.MovementCLI.Args.SET_SHORT, override(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, String.valueOf(TEST_SIZE + 1)),
                CLI.MovementCLI.Args.SET_SHORT, override(SuppliedWorkChunkDriver.Config.Keys.RANGE_TOP, String.valueOf(TEST_SIZE)),
                CLI.MovementCLI.Args.SET_SHORT, override(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName()),
                CLI.MovementCLI.Args.SET_SHORT, override(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName()),
        };

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));
//        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.TWO, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));

        MockUtil.setDefaultMockCallbacks();

        CLI.main(args);
    }

}
