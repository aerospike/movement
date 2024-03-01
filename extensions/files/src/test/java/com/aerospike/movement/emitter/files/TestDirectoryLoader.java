/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.files;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.files.csv.GraphCSVDecoder;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopGraphDecoder;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopGraphEncoder;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopTraversalEncoder;
import com.aerospike.movement.output.tinkerpop.TinkerPopGraphOutput;
import com.aerospike.movement.output.tinkerpop.TinkerPopTraversalOutput;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.impl.PassthroughOutputIdDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphTraversalProvider;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.tinkerpop.common.RemoteGraphTraversalProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.IOUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.files.FileUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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
import static com.aerospike.movement.output.files.DirectoryOutput.EDGES;
import static com.aerospike.movement.output.files.DirectoryOutput.VERTICES;
import static com.aerospike.movement.emitter.files.FileTestUtil.writeClassicGraphToDirectory;
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
        Configuration phaseOneConfig = ConfigUtil.withOverrides(testConfig, new HashMap<>() {{
            put(INTERNAL_PHASE_INDICATOR, "ONE");
        }});
        Configuration phaseTwoConfig = ConfigUtil.withOverrides(testConfig, new HashMap<>() {{
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

//        assertEquals(fileDataRowCount, driverElementCount);
    }


    @Test
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
                    put(DirectoryEmitter.Config.Keys.BASE_PATH, outputDirectory.toAbsolutePath().toString());
                    put(DirectoryEmitter.Config.Keys.PHASE_ONE_SUBDIR, "vertices");
                    put(DirectoryEmitter.Config.Keys.PHASE_TWO_SUBDIR, "edges");
                    put(WORK_CHUNK_DRIVER_PHASE_ONE, RecursiveDirectoryTraversalDriver.class.getName());
                    put(WORK_CHUNK_DRIVER_PHASE_TWO, RecursiveDirectoryTraversalDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, PassthroughOutputIdDriver.class.getName());
                }});

        System.out.println(ConfigUtil.configurationToPropertiesFormat(testConfig));

        final Runtime runtime = LocalParallelStreamRuntime.open(testConfig);

        final Iterator<RunningPhase> x = runtime.runPhases(
                List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO),
                testConfig);

        iteratePhasesAndCloseRuntime(x, runtime);

        long loadedVertices = SharedEmptyTinkerGraphGraphProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).traversal().V().count().next();
        long loadedEdges = SharedEmptyTinkerGraphGraphProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).traversal().E().count().next();
        System.out.printf("vertices loaded: %s%n", loadedVertices);
        System.out.printf("edges loaded: %s%n", loadedEdges);
        assertEquals(6, loadedVertices);
        assertEquals(6, loadedEdges);
        assertTrue(outputDirectory.resolve("edges").toFile().isDirectory());
        assertTrue(outputDirectory.resolve("vertices").toFile().isDirectory());
    }

    @Test
    public void testDirectoryLoaderDanglingEdgesGraph() throws IOException {
        SharedEmptyTinkerGraphGraphProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).traversal().V().drop().iterate();
        Path tempPath = IOUtil.createTempDir();
        assertTrue(tempPath.resolve(VERTICES).toFile().mkdir());
        assertTrue(tempPath.resolve(EDGES).toFile().mkdir());
        final String VERTEX_FILE = "classic_missing_peter.csv";
        final String EDGE_FILE = "classic_edges.csv";
        Files.move(
                IOUtil.copyFromResourcesIntoNewTempFile(
                        "missing_vertices/" + VERTEX_FILE,
                        VERTEX_FILE).toPath(),
                tempPath.resolve(VERTICES)
                        .resolve(VERTEX_FILE));
        Files.move(
                IOUtil.copyFromResourcesIntoNewTempFile(
                        "missing_vertices/" + EDGE_FILE,
                        EDGE_FILE).toPath(),
                tempPath.resolve(EDGES)
                        .resolve(EDGE_FILE));

        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);
                    put(EMITTER, DirectoryEmitter.class.getName());
                    put(DECODER, GraphCSVDecoder.class.getName());
                    put(ENCODER, TinkerPopGraphEncoder.class.getName());
                    put(OUTPUT, TinkerPopGraphOutput.class.getName());
                    put(TinkerPopGraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
                    put(DirectoryEmitter.Config.Keys.BASE_PATH, tempPath.toAbsolutePath().toString());
                    put(DirectoryEmitter.Config.Keys.PHASE_ONE_SUBDIR, VERTICES);
                    put(DirectoryEmitter.Config.Keys.PHASE_TWO_SUBDIR, EDGES);

                    put(TinkerPopGraphEncoder.Config.Keys.DROP_DANGLING_EDGES,true);
                    put(WORK_CHUNK_DRIVER_PHASE_ONE, RecursiveDirectoryTraversalDriver.class.getName());
                    put(WORK_CHUNK_DRIVER_PHASE_TWO, RecursiveDirectoryTraversalDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, PassthroughOutputIdDriver.class.getName());
                }});

        System.out.println(ConfigUtil.configurationToPropertiesFormat(testConfig));

        final Runtime runtime = LocalParallelStreamRuntime.open(testConfig);

        final Iterator<RunningPhase> x = runtime.runPhases(
                List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO),
                testConfig);

        iteratePhasesAndCloseRuntime(x, runtime);

        long loadedVertices = SharedEmptyTinkerGraphGraphProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).traversal().V().count().next();
        long loadedEdges = SharedEmptyTinkerGraphGraphProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).traversal().E().count().next();
        System.out.printf("vertices loaded: %s%n", loadedVertices);
        System.out.printf("edges loaded: %s%n", loadedEdges);
        assertEquals(5, loadedVertices);
        assertEquals(5, loadedEdges);
        assertTrue(tempPath.resolve("edges").toFile().isDirectory());
        assertTrue(tempPath.resolve("vertices").toFile().isDirectory());
    }

    @Test
    public void testDirectoryLoaderDanglingEdgesTraversal() throws IOException {
        SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).V().drop().iterate();
        Path tempPath = IOUtil.createTempDir();
        assertTrue(tempPath.resolve(VERTICES).toFile().mkdir());
        assertTrue(tempPath.resolve(EDGES).toFile().mkdir());
        final String VERTEX_FILE = "classic_missing_peter.csv";
        final String EDGE_FILE = "classic_edges.csv";
        Files.move(
                IOUtil.copyFromResourcesIntoNewTempFile(
                        "missing_vertices/" + VERTEX_FILE,
                        VERTEX_FILE).toPath(),
                tempPath.resolve(VERTICES)
                        .resolve(VERTEX_FILE));
        Files.move(
                IOUtil.copyFromResourcesIntoNewTempFile(
                        "missing_vertices/" + EDGE_FILE,
                        EDGE_FILE).toPath(),
                tempPath.resolve(EDGES)
                        .resolve(EDGE_FILE));

        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, 4);
                    put(EMITTER, DirectoryEmitter.class.getName());
                    put(DECODER, GraphCSVDecoder.class.getName());
                    put(ENCODER, TinkerPopTraversalEncoder.class.getName());
                    put(OUTPUT, TinkerPopTraversalOutput.class.getName());
                    put(TinkerPopTraversalEncoder.Config.Keys.TRAVERSAL_PROVIDER, SharedEmptyTinkerGraphTraversalProvider.class.getName());
                    put(DirectoryEmitter.Config.Keys.BASE_PATH, tempPath.toAbsolutePath().toString());
                    put(DirectoryEmitter.Config.Keys.PHASE_ONE_SUBDIR, VERTICES);
                    put(DirectoryEmitter.Config.Keys.PHASE_TWO_SUBDIR, EDGES);
                    put(WORK_CHUNK_DRIVER_PHASE_ONE, RecursiveDirectoryTraversalDriver.class.getName());
                    put(WORK_CHUNK_DRIVER_PHASE_TWO, RecursiveDirectoryTraversalDriver.class.getName());
                    put(TinkerPopTraversalEncoder.Config.Keys.DROP_DANGLING_EDGES, true);
                    put(OUTPUT_ID_DRIVER, PassthroughOutputIdDriver.class.getName());
                }});

        System.out.println(ConfigUtil.configurationToPropertiesFormat(testConfig));

        final Runtime runtime = LocalParallelStreamRuntime.open(testConfig);

        final Iterator<RunningPhase> x = runtime.runPhases(
                List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO),
                testConfig);

        iteratePhasesAndCloseRuntime(x, runtime);

        runtime.close();
        long loadedVertices = SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).V().count().next();
        long loadedEdges = SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).E().count().next();
        System.out.printf("vertices loaded: %s%n", loadedVertices);
        System.out.printf("edges loaded: %s%n", loadedEdges);
        assertEquals(5, loadedVertices);
        assertEquals(5, loadedEdges);
        assertTrue(tempPath.resolve("edges").toFile().isDirectory());
        assertTrue(tempPath.resolve("vertices").toFile().isDirectory());
    }

    @Test
    @Ignore
    public void testDirectoryLoaderExt() throws IOException {
        final String host = "172.17.0.1";
        final int port = 8182;
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                DriverRemoteConnection.using(host, port, "g"));
        g.V().drop().iterate();
        Path tempPath = Path.of("/data/files/all");
//        assertTrue(tempPath.resolve(VERTICES).toFile().mkdir());
//        assertTrue(tempPath.resolve(EDGES).toFile().mkdir());
//        final String VERTEX_FILE = "classic_missing_peter.csv";
//        final String EDGE_FILE = "classic_edges.csv";
//        Files.move(
//                IOUtil.copyFromResourcesIntoNewTempFile(
//                        "missing_vertices/" + VERTEX_FILE,
//                        VERTEX_FILE).toPath(),
//                tempPath.resolve(VERTICES)
//                        .resolve(VERTEX_FILE));
//        Files.move(
//                IOUtil.copyFromResourcesIntoNewTempFile(
//                        "missing_vertices/" + EDGE_FILE,
//                        EDGE_FILE).toPath(),
//                tempPath.resolve(EDGES)
//                        .resolve(EDGE_FILE));

        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, RuntimeUtil.getAvailableProcessors());
                    put(EMITTER, DirectoryEmitter.class.getName());
                    put(DECODER, GraphCSVDecoder.class.getName());
                    put(ENCODER, TinkerPopTraversalEncoder.class.getName());
                    put(OUTPUT, TinkerPopTraversalOutput.class.getName());
                    put(TinkerPopTraversalEncoder.Config.Keys.TRAVERSAL_PROVIDER, RemoteGraphTraversalProvider.class.getName());
                    put(RemoteGraphTraversalProvider.Config.Keys.HOST, host);
                    put(RemoteGraphTraversalProvider.Config.Keys.PORT, port);
                    put(DirectoryEmitter.Config.Keys.BASE_PATH, tempPath.toAbsolutePath().toString());
                    put(DirectoryEmitter.Config.Keys.PHASE_ONE_SUBDIR, VERTICES);
                    put(DirectoryEmitter.Config.Keys.PHASE_TWO_SUBDIR, EDGES);
                    put(WORK_CHUNK_DRIVER_PHASE_ONE, RecursiveDirectoryTraversalDriver.class.getName());
                    put(WORK_CHUNK_DRIVER_PHASE_TWO, RecursiveDirectoryTraversalDriver.class.getName());
                    put(TinkerPopTraversalEncoder.Config.Keys.DROP_DANGLING_EDGES, true);
                    put(OUTPUT_ID_DRIVER, PassthroughOutputIdDriver.class.getName());
                }});

        System.out.println(ConfigUtil.configurationToPropertiesFormat(testConfig));

        final Runtime runtime = LocalParallelStreamRuntime.open(testConfig);

        final Iterator<RunningPhase> x = runtime.runPhases(
                List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO),
                testConfig);

        iteratePhasesAndCloseRuntime(x, runtime);

        runtime.close();
        long loadedVertices = g.V().count().next();
        long loadedEdges = g.E().count().next();
        System.out.printf("vertices loaded: %s%n", loadedVertices);
        System.out.printf("edges loaded: %s%n", loadedEdges);
//        assertEquals(5, loadedVertices);
//        assertEquals(5, loadedEdges);
//        assertTrue(tempPath.resolve("edges").toFile().isDirectory());
//        assertTrue(tempPath.resolve("vertices").toFile().isDirectory());
    }
}
