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
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.runtime.tinkerpop.TinkerPopGraphDriver;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.tinkerpop.SharedTinkerClassicGraphProvider;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphTraversalProvider;
import com.aerospike.movement.test.tinkerpop.TinkerModernGraphProvider;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.tinkerpop.common.RemoteGraphTraversalProvider;
import com.aerospike.movement.tinkerpop.common.TraversalProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
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
        SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).V().drop().iterate();
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
        put(TinkerPopGraphEmitter.Config.Keys.GRAPH_PROVIDER, SharedTinkerClassicGraphProvider.class.getName());

        put(ConfigurationBase.Keys.ENCODER, TinkerPopTraversalEncoder.class.getName());
        put(TinkerPopTraversalEncoder.Config.Keys.TRAVERSAL_PROVIDER, SharedEmptyTinkerGraphTraversalProvider.class.getName());


        put(ConfigurationBase.Keys.OUTPUT, TinkerPopTraversalOutput.class.getName());
    }});


    @Test
    public void testWillTransferVerticesFromGraphAToGraphBbyTraversal() {


        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).E().count().next().longValue());
        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).V().count().next().longValue());

        final Configuration config = ConfigUtil.withOverrides(graphTransferConfig, new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, TinkerPopGraphDriver.class.getName());
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO, TinkerPopGraphDriver.class.getName());

            put(THREADS, 1); //TinkerGraph is single threaded
        }}));


        registerCleanupCallback(() -> {
            SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).V().drop().iterate();
            LocalParallelStreamRuntime.getInstance(config).close();
        });


        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);

        iteratePhasesTimed(runtime, List.of(Runtime.PHASE.ONE), config);

        assertEquals(PHASE_ONE_TEST_SIZE, SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).V().count().next().longValue());
    }


    @Test
    public void loopVertexTest() {
        IntStream.range(0, 1000).forEach(i -> {
            testWillTransferVerticesFromGraphAToGraphBbyTraversal();
            cleanup();
            cleanGraph();
            setup();
        });
    }

    @Test
    public void testWillTransferEdgesFromGraphAToGraphBByTraversal() {
        TraversalProvider provider = SharedEmptyTinkerGraphTraversalProvider.open();
        GraphTraversalSource outputSink = provider.getProvided(GraphProvider.GraphProviderContext.OUTPUT);
        assertEquals(0L, outputSink.E().count().next().longValue());
        assertEquals(0L, outputSink.V().count().next().longValue());
        final Configuration config = ConfigUtil.withOverrides(graphTransferConfig, new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, TinkerPopGraphDriver.class.getName());
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO, TinkerPopGraphDriver.class.getName());
        }}));
        registerCleanupCallback(() -> {
//            SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).V().drop().iterate();
            LocalParallelStreamRuntime.getInstance(config).close();
        });


        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);
        final Iterator<RunningPhase> x = runtime.runPhases(
                List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO),
                config);
        iteratePhasesAndCloseRuntime(x, runtime);

        assertEquals(PHASE_TWO_TEST_SIZE, outputSink.E().count().next().longValue());
        try {
            outputSink.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final String REMOTE_TRAVERSAL_TARGET = "graph.synth.remote.target";

    @Test
    public void testWillTransferEdgesFromGraphAToGraphBByRemoteURITraversal() {

        final RemoteGraphTraversalProvider.URIConnectionInfo info;
        final GraphTraversalSource remote;
        if (RuntimeUtil.envOrProperty(REMOTE_TRAVERSAL_TARGET).isEmpty()) {
            URI localhostURI = URI.create("ws://localhost:8182/g");

            info = RemoteGraphTraversalProvider.URIConnectionInfo.from(localhostURI);

        } else {
            final URI remoteUri = URI.create(RuntimeUtil.envOrProperty(REMOTE_TRAVERSAL_TARGET).get());
            info = RemoteGraphTraversalProvider.URIConnectionInfo.from(remoteUri);
        }
        try {
            remote = AnonymousTraversalSource
                    .traversal()
                    .withRemote(DriverRemoteConnection.using(info.host, info.port, info.traversalSourceName));
            remote.V().limit(1).hasNext();

        }catch (Exception e){
            System.out.println("skipping test, could not connect to " + info.toString());
            return;
        }
        final Configuration remoteGraphTransferConfig = new MapConfiguration(new HashMap<>() {{
            put(THREADS, String.valueOf(THREAD_COUNT));

            put(ConfigurationBase.Keys.EMITTER, TinkerPopGraphEmitter.class.getName());
            put(TinkerPopGraphEmitter.Config.Keys.GRAPH_PROVIDER, TinkerModernGraphProvider.class.getName());

            put(ConfigurationBase.Keys.ENCODER, TinkerPopTraversalEncoder.class.getName());
            put(TinkerPopTraversalEncoder.Config.Keys.TRAVERSAL_PROVIDER, RemoteGraphTraversalProvider.class.getName());

            put(RemoteGraphTraversalProvider.Config.Keys.OUTPUT_URI, info.uri().toString());

            put(ConfigurationBase.Keys.OUTPUT, TinkerPopTraversalOutput.class.getName());
        }});
        remote.V().drop().iterate();
        assertEquals(0L, remote.E().count().next().longValue());
        assertEquals(0L, remote.V().count().next().longValue());
        final Configuration config = ConfigUtil.withOverrides(remoteGraphTransferConfig, new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, TinkerPopGraphDriver.class.getName());
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_TWO, TinkerPopGraphDriver.class.getName());
        }}));
        registerCleanupCallback(() -> {
            GraphTraversalSource x = AnonymousTraversalSource
                    .traversal()
                    .withRemote(DriverRemoteConnection.using(info.host, info.port, info.traversalSourceName));
            x.V().drop().iterate();
            try {
                x.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LocalParallelStreamRuntime.getInstance(config).close();
        });


        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);
        final Iterator<RunningPhase> x = runtime.runPhases(
                List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO),
                config);
        iteratePhasesAndCloseRuntime(x, runtime);

        assertEquals(TinkerFactory.createModern().traversal().E().count().next().longValue(), remote.E().count().next().longValue());
        assertEquals(TinkerFactory.createModern().traversal().V().count().next().longValue(), remote.V().count().next().longValue());

        try {
            remote.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void loopEdgeTest() {
        SharedEmptyTinkerGraphTraversalProvider.open().getProvided(GraphProvider.GraphProviderContext.OUTPUT).V().drop().iterate();
        IntStream.range(0, 1000).forEach(i -> {
            testWillTransferEdgesFromGraphAToGraphBByTraversal();
            cleanup();
            cleanGraph();
            setup();
        });
    }


}
