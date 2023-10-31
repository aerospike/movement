/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.output.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.tinkerpop.TinkerPopGraphEmitter;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopTraversalEncoder;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.tinkerpop.ClassicGraphProvider;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphTraversalProvider;
import com.aerospike.movement.tinkerpop.common.RemoteGraphTraversalProvider;
import com.aerospike.movement.tinkerpop.common.TinkerPopGraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;
import com.aerospike.movement.util.core.runtime.IOUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.THREADS;
import static junit.framework.TestCase.assertEquals;

public class TestTinkerPopTraversalOutput extends AbstractMovementTest {
    final int THREAD_COUNT = 1; //TinkerGraph is single threaded
    final static long PHASE_ONE_TEST_SIZE = TinkerFactory.createClassic().traversal().V().count().next();
    final static long PHASE_TWO_TEST_SIZE = TinkerFactory.createClassic().traversal().E().count().next();

    @Before
    @After
    public void cleanGraph() {
        SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().drop().iterate();
    }


    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }


    final Configuration graphTransferConfig = new MapConfiguration(new HashMap<>() {{
        put(THREADS, String.valueOf(THREAD_COUNT));
        put(ConfigurationBase.Keys.EMITTER, TinkerPopGraphEmitter.class.getName());
        put(TinkerPopGraphEmitter.Config.Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
        put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, ClassicGraphProvider.class.getName());

        put(ConfigurationBase.Keys.ENCODER, TinkerPopTraversalEncoder.class.getName());
        put(TinkerPopTraversalEncoder.Config.Keys.TRAVERSAL_PROVIDER, SharedEmptyTinkerGraphTraversalProvider.class.getName());
        put(ConfigurationBase.Keys.OUTPUT, TinkerPopTraversalOutput.class.getName());
    }});


    @Test
    public void testWillTransferVerticesFromGraphAToGraphBbyTraversal() {

        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().E().count().next().longValue());
        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().count().next().longValue());

        final Configuration config = ConfigurationUtil.configurationWithOverrides(graphTransferConfig, new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, SuppliedWorkChunkDriver.class.getName());
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO, SuppliedWorkChunkDriver.class.getName());

            put(THREADS, 1); //TinkerGraph is single threaded
        }}));


        registerCleanupCallback(() -> {
            SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().drop().iterate();
            LocalParallelStreamRuntime.getInstance(config).close();
        });


        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotIteratorSupplier.of(() ->
                TinkerFactory.createClassic().traversal().V().id()));


        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);

        iteratePhasesTimed(runtime, List.of(Runtime.PHASE.ONE), config);

        assertEquals(PHASE_ONE_TEST_SIZE, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().count().next().longValue());
    }


    @Test
    public void loopVertexTest() {
        IntStream.range(0, 1000).forEach(i -> {
            testWillTransferVerticesFromGraphAToGraphBbyTraversal();
            cleanup();
            setup();
        });
    }

    @Test
    public void testWillTransferEdgesFromGraphAToGraphBByTraversal() {
        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().E().count().next().longValue());
        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().count().next().longValue());
        final Configuration config = ConfigurationUtil.configurationWithOverrides(graphTransferConfig, new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, SuppliedWorkChunkDriver.class.getName());
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO, SuppliedWorkChunkDriver.class.getName());
        }}));
        registerCleanupCallback(() -> {
            SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().drop().iterate();
            LocalParallelStreamRuntime.getInstance(config).close();
        });

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotIteratorSupplier.of(() ->
                TinkerFactory.createClassic().traversal().V().id()));

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.TWO, OneShotIteratorSupplier.of(() ->
                TinkerFactory.createClassic().traversal().E().id()));

        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);

        iteratePhasesTimed(runtime, List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO), config);

        assertEquals(PHASE_TWO_TEST_SIZE, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().E().count().next().longValue());
    }

    @Test
    public void loopEdgeTest() {
        IntStream.range(0, 1000).forEach(i -> {
            testWillTransferEdgesFromGraphAToGraphBByTraversal();
            cleanup();
            setup();
        });
    }


    @Test
    @Ignore
    public void testExternalConfig() throws IOException {
        final Long SCALE_FACTOR = 100L;
        final File schemaFile = IOUtil.copyFromResourcesIntoNewTempFile("gdemo_schema.yaml");

        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
//                    put(YAMLSchemaParser.Config.Keys.YAML_FILE_PATH, (String) schemaFile.getAbsolutePath());
                    put(RemoteGraphTraversalProvider.Config.Keys.HOST, "localhost");
                    put(RemoteGraphTraversalProvider.Config.Keys.PORT, 8182);
                }});
        System.out.println(ConfigurationUtil.configurationToPropertiesFormat(testConfig));

        final GraphTraversalSource g = AnonymousTraversalSource
                .traversal()
                .withRemote(DriverRemoteConnection
                        .using(testConfig.getString(RemoteGraphTraversalProvider.Config.Keys.HOST),
                                Integer.parseInt(testConfig.getString(RemoteGraphTraversalProvider.Config.Keys.PORT)),
                                "g"));
        g.V().drop().iterate();


        final Runtime runtime = LocalParallelStreamRuntime.open(testConfig);
        final Iterator<RunningPhase> x = runtime.runPhases(List.of(Runtime.PHASE.ONE), testConfig);
        while (x.hasNext()) {
            final RunningPhase y = x.next();
            IteratorUtils.iterate(y);
            y.get();
            y.close();
        }
        runtime.close();
        assertEquals(1700L, g.V().count().next().longValue());
        assertEquals(1600L, g.E().count().next().longValue());
    }
}
