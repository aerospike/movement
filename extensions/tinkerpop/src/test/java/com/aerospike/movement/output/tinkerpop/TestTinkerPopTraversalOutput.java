package com.aerospike.movement.output.tinkerpop;

import com.aerospike.movement.tinkerpop.common.instrumentation.TinkerPopGraphProvider;
import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.iterator.OneShotSupplier;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.iterator.PrimitiveIteratorWrap;
import com.aerospike.movement.emitter.tinkerpop.SourceGraph;
import com.aerospike.movement.encoding.tinkerpop.TraversalEncoder;
import com.aerospike.movement.test.tinkerpop.ClassicGraph;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphTraversalProvider;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

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


    final Configuration graphTransferConfig = new MapConfiguration(new HashMap<>() {{
        put(THREADS, String.valueOf(THREAD_COUNT));
        put(ConfigurationBase.Keys.EMITTER, SourceGraph.class.getName());
        put(SourceGraph.Config.Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
        put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, ClassicGraph.class.getName());

        put(ConfigurationBase.Keys.ENCODER, TraversalEncoder.class.getName());
        put(TraversalEncoder.Config.Keys.TRAVERSAL_PROVIDER, SharedEmptyTinkerGraphTraversalProvider.class.getName());
        put(ConfigurationBase.Keys.OUTPUT, TraversalOutput.class.getName());
    }});


    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testWillTransferVerticesFromGraphAToGraphBbyTraversal() {

        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().E().count().next().longValue());
        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().count().next().longValue());

        final Configuration config = ConfigurationUtil.configurationWithOverrides(graphTransferConfig, new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
        }}));


        registerCleanupCallback(() -> {
            SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().drop().iterate();
            LocalParallelStreamRuntime.getInstance(config).close();
        });


        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() ->
                TinkerFactory.createClassic().traversal().V().id()));


        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);

        integrationTest(runtime, List.of(Runtime.PHASE.ONE), config);

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
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
        }}));
        registerCleanupCallback(() -> {
            SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().drop().iterate();
            LocalParallelStreamRuntime.getInstance(config).close();
        });

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() ->
                TinkerFactory.createClassic().traversal().V().id()));

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.TWO, OneShotSupplier.of(() ->
                TinkerFactory.createClassic().traversal().E().id()));

        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);

        integrationTest(runtime, List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO), config);

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

}
