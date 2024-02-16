/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.cli;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.output.files.DirectoryOutput;
import com.aerospike.movement.plugin.cli.CLIPlugin;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.GeneratedOutputIdDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.encoder.MockEncoder;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.test.mock.task.MockTask;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.THREADS;
import static com.aerospike.movement.test.mock.MockUtil.getHitCounter;
import static com.aerospike.movement.util.core.runtime.RuntimeUtil.getAvailableProcessors;
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
        assertEquals(5, z.size());
        assertEquals(z.size(), y.size());
        CLI.main(args);
    }

    @Test
    public void testListComponents() throws Exception {
        RuntimeUtil.loadClass(MockTask.class.getName());
        final Map<String, Class<? extends Task>> tasks = RuntimeUtil.getTasks();
        assertTrue(tasks.containsKey(MockTask.class.getSimpleName()));

        final String[] args = {CLI.MovementCLI.Args.TEST_MODE, CLI.MovementCLI.Args.LIST_COMPONENTS};
        final Optional<CLIPlugin> x = CLI.parseAndLoadPlugin(args);
        assertTrue(x.isPresent());
        final List<Object> y = IteratorUtils.list(x.get().call());
        for (Object s : y) {
            System.out.println(s);
        }
        assertTrue(y.size() > 3);
        CLI.main(args);
    }

    private final Integer TEST_SIZE = 10_000;





    @Test
    public void testRunMockTask() throws Exception {
        assertEquals(0, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
        RuntimeUtil.loadClass(MockTask.class.getName());
        final Path tmpConfig = Files.createTempFile("movement", "test").toAbsolutePath();
        final String configString = ConfigUtil.configurationToPropertiesFormat(getMockConfiguration(new HashMap<>()));
        Files.write(tmpConfig, configString.getBytes());


        final String[] args = {
                CLI.MovementCLI.Args.TEST_MODE,
                CLI.MovementCLI.Args.TASK, MockTask.class.getSimpleName(),
                CLI.MovementCLI.Args.CONFIG_SHORT, tmpConfig.toString(),
                CLI.MovementCLI.Args.DEBUG_SHORT,
                CLI.MovementCLI.Args.SET_SHORT, CLI.setEquals(THREADS, "1"),
                CLI.MovementCLI.Args.SET_SHORT, CLI.setEquals(BATCH_SIZE, String.valueOf(TEST_SIZE / getAvailableProcessors() / 8)),
                CLI.MovementCLI.Args.SET_SHORT, CLI.setEquals(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, SuppliedWorkChunkDriver.class.getName()),
                CLI.MovementCLI.Args.SET_SHORT, CLI.setEquals(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName()),
        };
        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotIteratorSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));
        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.TWO, OneShotIteratorSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, TEST_SIZE).iterator())));

        MockUtil.setDefaultMockCallbacks();

        final Optional<CLIPlugin> x = CLI.parseAndLoadPlugin(args);
        assertTrue(x.isPresent());
        final UUID id = (UUID) x.get().call().next();
        Iterator<Map<String, Object>> iterator = RuntimeUtil.statusIteratorForTask(id);
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            Thread.sleep(1000L);
        }
        RuntimeUtil.waitTask(id);
        LocalParallelStreamRuntime.closeStatic();

        final int NUMBER_OF_PHASES = 1;
        TestCase.assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockEncoder.class, MockEncoder.Methods.ENCODE));
        TestCase.assertEquals(TEST_SIZE * NUMBER_OF_PHASES, getHitCounter(MockOutput.class, MockOutput.Methods.WRITE_TO_OUTPUT));
    }

    @Test
    @Ignore //@todo generator extraction
    public void testRunExampleConfigurationGenerate() throws Exception {
        long TEST_SIZE = 10;
        RuntimeUtil.loadClass(MockTask.class.getName());
        final Path exampleConfig = Path.of("../conf/generator/example.properties");

        final String[] args = {
                CLI.MovementCLI.Args.TEST_MODE,
//                CLI.MovementCLI.Args.TASK, Generate.class.getSimpleName(),
                CLI.MovementCLI.Args.CONFIG_SHORT, exampleConfig.toString(),
                CLI.MovementCLI.Args.DEBUG_SHORT,
                CLI.MovementCLI.Args.SET_SHORT, CLI.setEquals(BATCH_SIZE, "1"),
                CLI.MovementCLI.Args.SET_SHORT, CLI.setEquals(THREADS, "1"),
//                CLI.MovementCLI.Args.SET_SHORT, CLI.setEquals(SCALE_FACTOR, String.valueOf(TEST_SIZE)),
//                CLI.MovementCLI.Args.SET_SHORT, CLI.setEquals(YAMLSchemaParser.Config.Keys.YAML_FILE_PATH, "../extensions/generator/src/main/resources/example_schema.yaml"),
                CLI.MovementCLI.Args.SET_SHORT, CLI.setEquals(DirectoryOutput.Config.Keys.DIRECTORY, "/tmp/generate")
        };

        MockUtil.setDefaultMockCallbacks();

        CLI.main(args);
        System.out.println(String.join(" ", args));

    }

}
