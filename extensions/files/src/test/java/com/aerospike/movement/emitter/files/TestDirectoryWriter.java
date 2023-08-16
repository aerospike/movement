package com.aerospike.movement.emitter.files;

import com.aerospike.movement.emitter.generator.Generator;
import com.aerospike.movement.emitter.generator.schema.YAMLParser;
import com.aerospike.movement.encoding.files.csv.GraphCSVEncoder;
import com.aerospike.movement.output.files.DirectoryOutput;
import com.aerospike.movement.output.files.SplitFileLineOutput;
import com.aerospike.movement.plugin.tinkerpop.PluginEnabledGraph;
import com.aerospike.movement.process.tasks.generator.Generate;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.GeneratedOutputIdDriver;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.emitter.MockEmitable;
import com.aerospike.movement.test.mock.encoder.MockEncoder;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.IOUtil;
import com.aerospike.movement.util.core.iterator.ConfiguredRangeSupplier;
import com.aerospike.movement.util.files.FileUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.*;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDirectoryWriter extends AbstractMovementTest {
    final Path outputDirectory = Path.of(System.getProperty("java.io.tmpdir")).resolve("generate");

    @Before
    public void cleanDirectory() {
        FileUtil.recursiveDelete(outputDirectory);
    }

    @Test
    public void testWriteFile() throws IOException {
        FileUtil.recursiveDelete(outputDirectory);
        MockUtil.setDefaultMockCallbacks();
        final Configuration testConfig = getMockConfiguration(new HashMap<>() {{
            put(DirectoryOutput.Config.Keys.DIRECTORY, outputDirectory.toAbsolutePath().toString());
        }});

        final MockEncoder<String> enc = MockEncoder.open(testConfig);

        final AtomicLong fileMetric = new AtomicLong(0L);
        final String valueToWrite = "a";
        final String headerToWrite = "b";
        outputDirectory.toFile().mkdir();
        outputDirectory.resolve("test").toFile().mkdir();
        final SplitFileLineOutput x = SplitFileLineOutput.create("test", enc, fileMetric, testConfig);
        x.write(valueToWrite, headerToWrite);
        x.close();
        assertEquals(1, Files.walk(outputDirectory).filter(it -> it.toString().endsWith(".csv")).count());
        assertEquals(1, Files.lines(outputDirectory.resolve("test").resolve("test_1.csv"))
                .filter(line -> line.contains(valueToWrite)).count());
    }

    @Test
    public void testWriteDirectory() throws IOException {
        SuppliedWorkChunkDriver.clearSupplierForPhase(Runtime.PHASE.ONE);
        SuppliedWorkChunkDriver.clearSupplierForPhase(Runtime.PHASE.TWO);

        FileUtil.recursiveDelete(outputDirectory);

        MockUtil.setDefaultMockCallbacks();
        final Configuration testConfig = getMockConfiguration(new HashMap<>() {{
            put(DirectoryOutput.Config.Keys.DIRECTORY, outputDirectory.toAbsolutePath().toString());
        }});

        MockEmitable emitable = new MockEmitable("z", true, ConfigurationUtil.empty());

        final DirectoryOutput o = DirectoryOutput.open(testConfig);
        o.writer(MockEmitable.class, "b").writeToOutput(emitable);
        o.close();
        assertEquals(1, Files.walk(outputDirectory).filter(it -> it.toString().endsWith(".csv")).count());
        Files.walk(outputDirectory).forEach(System.out::println);
        assertTrue(outputDirectory.resolve("mock").toFile().isDirectory());
        assertTrue(outputDirectory.resolve("mock").resolve("b").toFile().isDirectory());
        assertTrue(outputDirectory.resolve("mock").resolve("b").resolve("b_1.csv").toFile().exists());
        Files.walk(outputDirectory).forEach(System.out::println);
    }


    @Test
    public void testEmitToDirectory() throws IOException {
        FileUtil.recursiveDelete(outputDirectory);

        final Long SCALE_FACTOR = 100L;
        final File schemaFile = IOUtil.copyFromResourcesIntoNewTempFile("faker_schema.yaml");

        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);
                    put(EMITTER, Generator.class.getName());
                    put(ENCODER, GraphCSVEncoder.class.getName());
                    put(OUTPUT, DirectoryOutput.class.getName());

                    put(YAMLParser.Config.Keys.YAML_FILE_PATH, schemaFile.getAbsolutePath());

                    put(DirectoryOutput.Config.Keys.DIRECTORY, outputDirectory.toString());
                    put(WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());


                    put(SuppliedWorkChunkDriver.Config.Keys.ITERATOR_SUPPLIER, ConfiguredRangeSupplier.class.getName());

                    put(SuppliedWorkChunkDriver.Config.Keys.RANGE_BOTTOM, 0L);
                    put(SuppliedWorkChunkDriver.Config.Keys.RANGE_TOP, SCALE_FACTOR);
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, SCALE_FACTOR * 10);
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, Long.MAX_VALUE);
                }});
        System.out.println(ConfigurationUtil.configurationToPropertiesFormat(testConfig));


        final Runtime runtime = LocalParallelStreamRuntime.open(testConfig);
        final Iterator<RunningPhase> x = runtime.runPhases(List.of(Runtime.PHASE.ONE), testConfig);
        while (x.hasNext()) {
            final RunningPhase y = x.next();
            y.get();
            y.close();
        }
        runtime.close();
        assertTrue(outputDirectory.resolve("edges").toFile().isDirectory());
        assertTrue(outputDirectory.resolve("vertices").toFile().isDirectory());
        assertEquals(2, Files.list(outputDirectory).count());
        assertEquals(31, Files.walk(outputDirectory).count());
    }


    @Test
    @Ignore
    public void testGenerateToCSV() throws IOException {
        FileUtil.recursiveDelete(outputDirectory);
        final Long SCALE_FACTOR = 100L;
        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);
                    put(PluginEnabledGraph.Config.Keys.GRAPH_IMPL, SharedEmptyTinkerGraphGraphProvider.class.getName());
                    put(OUTPUT, DirectoryOutput.class.getName());
                    put(DirectoryOutput.Config.Keys.DIRECTORY, outputDirectory.toString());
                    put(WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());

                    put(EMITTER, Generator.class.getName());
                    put(ENCODER, GraphCSVEncoder.class.getName());
                    put(SuppliedWorkChunkDriver.Config.Keys.ITERATOR_SUPPLIER, ConfiguredRangeSupplier.class.getName());

                    put(SuppliedWorkChunkDriver.Config.Keys.RANGE_BOTTOM, 0L);
                    put(SuppliedWorkChunkDriver.Config.Keys.RANGE_TOP, SCALE_FACTOR);
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, SCALE_FACTOR * 10);
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, Long.MAX_VALUE);
                }});

        final Graph controlGraph = PluginEnabledGraph.open(testConfig);
        final GraphTraversalSource controlG = controlGraph.traversal();


        final File schemaFile = IOUtil.copyFromResourcesIntoNewTempFile("example_schema.yaml");

        final Object x = controlG
                .call(Generate.class.getSimpleName())
                .with(Generator.Config.Keys.SCALE_FACTOR, SCALE_FACTOR)
                .with(YAMLParser.Config.Keys.YAML_FILE_URI, schemaFile.toURI())
                .next();

        assertEquals(29, Files.walk(outputDirectory).count());
    }
}
