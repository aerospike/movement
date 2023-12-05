/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.files;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.files.csv.GraphCSVDecoder;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopGraphEncoder;
import com.aerospike.movement.output.tinkerpop.TinkerPopGraphOutput;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.impl.PassthroughOutputIdDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.files.FileUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.*;
import static com.aerospike.movement.test.files.FileTestUtil.writeClassicGraphToDirectory;
import static com.aerospike.movement.test.core.AbstractMovementTest.iteratePhasesAndCloseRuntime;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDirectoryLoader {
//    @Before
//    public void clean(){
//        LocalParallelStreamRuntime.closeStatic();
//    }

    @Test
    public void testRecursiveDirectoryTraversalDriver() throws IOException {
        final Path outputDirectory = Path.of(System.getProperty("java.io.tmpdir")).resolve("generate");
        FileUtil.recursiveDelete(outputDirectory);

        writeClassicGraphToDirectory(outputDirectory);

        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);
                    put(EMITTER, DirectoryEmitter.class.getName());
                    put(DECODER, GraphCSVDecoder.class.getName());
                    put(ENCODER, TinkerPopGraphEncoder.class.getName());
                    put(OUTPUT, TinkerPopGraphOutput.class.getName());
                    put(TinkerPopGraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
                    put(DirectoryEmitter.Config.Keys.BASE_PATH, "/tmp/generate");
                    put(DirectoryEmitter.Config.Keys.PHASE_ONE_SUBDIR, "vertices");
                    put(DirectoryEmitter.Config.Keys.PHASE_TWO_SUBDIR, "edges");
                    put(WORK_CHUNK_DRIVER_PHASE_ONE, RecursiveDirectoryTraversalDriver.class.getName());
                    put(WORK_CHUNK_DRIVER_PHASE_TWO, RecursiveDirectoryTraversalDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, PassthroughOutputIdDriver.class.getName());
                }});
        Configuration phaseOneConfig = ConfigurationUtil.configurationWithOverrides(testConfig, new HashMap<>() {{
            put(INTERNAL_PHASE_INDICATOR, "ONE");
        }});
        Configuration phaseTwoConfig = ConfigurationUtil.configurationWithOverrides(testConfig, new HashMap<>() {{
            put(INTERNAL_PHASE_INDICATOR, "TWO");
        }});

        RecursiveDirectoryTraversalDriver driver = RecursiveDirectoryTraversalDriver.open(phaseOneConfig);
        driver.init(phaseOneConfig);
        assertTrue(driver.getInitialized().get());

        final List<WorkChunk> chunks = Stream.iterate(driver.getNext(), Optional::isPresent, i -> driver.getNext())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());


        long driverElementCount = IteratorUtils.count(IteratorUtils.flatMap(chunks.iterator(), chunk -> ((Emitable) chunk).emit(RuntimeUtil.loadOutput(testConfig)).iterator()));
        long fileDataRowCount = Files.walk(DirectoryEmitter.getPhasePath(Runtime.PHASE.ONE, testConfig))
                .filter(it -> !it.toFile().isDirectory())
                .flatMap(it -> {
                    try {
                        return Files.lines(it).skip(1);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .count();

        assertEquals(fileDataRowCount, driverElementCount);
    }


    @Test
    @Ignore //@todo fix
    public void highLevelDirectoryLoaderTest() throws IOException {
        final Path outputDirectory = Path.of(System.getProperty("java.io.tmpdir")).resolve("generate");
        FileUtil.recursiveDelete(outputDirectory);

        writeClassicGraphToDirectory(outputDirectory);

        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);
                    put(EMITTER, DirectoryEmitter.class.getName());
                    put(DECODER, GraphCSVDecoder.class.getName());
                    put(ENCODER, TinkerPopGraphEncoder.class.getName());
                    put(OUTPUT, TinkerPopGraphOutput.class.getName());
                    put(TinkerPopGraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
                    put(DirectoryEmitter.Config.Keys.BASE_PATH, "/tmp/generate");
                    put(DirectoryEmitter.Config.Keys.PHASE_ONE_SUBDIR, "vertices");
                    put(DirectoryEmitter.Config.Keys.PHASE_TWO_SUBDIR, "edges");
                    put(WORK_CHUNK_DRIVER_PHASE_ONE, RecursiveDirectoryTraversalDriver.class.getName());
                    put(WORK_CHUNK_DRIVER_PHASE_TWO, RecursiveDirectoryTraversalDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, PassthroughOutputIdDriver.class.getName());
                }});

        System.out.println(ConfigurationUtil.configurationToPropertiesFormat(testConfig));

        final Runtime runtime = LocalParallelStreamRuntime.open(testConfig);

        final Iterator<RunningPhase> x = runtime.runPhases(
                List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO),
                testConfig);

        iteratePhasesAndCloseRuntime(x,runtime);


        System.out.println(String.format("vertices loaded: " + SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().count().next()));
        System.out.println(String.format("edges loaded: " + SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().E().count().next()));

        assertTrue(outputDirectory.resolve("edges").toFile().isDirectory());
        assertTrue(outputDirectory.resolve("vertices").toFile().isDirectory());
    }


}
