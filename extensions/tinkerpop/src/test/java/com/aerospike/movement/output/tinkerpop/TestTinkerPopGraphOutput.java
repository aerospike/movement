package com.aerospike.movement.output.tinkerpop;

import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphTraversalProvider;
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
import com.aerospike.movement.encoding.tinkerpop.GraphEncoder;
import com.aerospike.movement.test.tinkerpop.ClassicGraph;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.THREADS;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class TestTinkerPopGraphOutput extends AbstractMovementTest {
    final int THREAD_COUNT = 1; //TinkerGraph is single threaded
    final static long PHASE_ONE_TEST_SIZE = TinkerFactory.createClassic().traversal().V().count().next();
    final static long PHASE_TWO_TEST_SIZE = TinkerFactory.createClassic().traversal().E().count().next();

    final Configuration graphTransferConfig = new MapConfiguration(new HashMap<>() {{
        put(THREADS, String.valueOf(THREAD_COUNT));
        put(ConfigurationBase.Keys.EMITTER, SourceGraph.class.getName());
        put(SourceGraph.Config.Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
        put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, ClassicGraph.class.getName());

        put(ConfigurationBase.Keys.ENCODER, GraphEncoder.class.getName());
        put(GraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
        put(ConfigurationBase.Keys.OUTPUT, GraphOutput.class.getName());
    }});

    AtomicLong setupCalls = new AtomicLong(0);
    AtomicLong cleanupCalls = new AtomicLong(0);

    @Before
    public void setup() {
        setupCalls.incrementAndGet();
        super.setup();
        SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().drop().iterate();
        SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().drop().iterate();
    }

    @After
    public void cleanup() {
        cleanupCalls.incrementAndGet();
        SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().drop().iterate();
        SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().drop().iterate();
        super.cleanup();
    }

    @Test
    public void testWillTransferVerticesFromGraphAToGraphB() {
        assertEquals((Long) 0L, (Long) SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().count().next());
        final Configuration config = ConfigurationUtil.configurationWithOverrides(graphTransferConfig, new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
        }}));
        registerCleanupCallback(() -> {
            SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().drop().iterate();
            LocalParallelStreamRuntime.getInstance(config).close();
        });

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() ->
                TinkerFactory.createClassic().traversal().V().id()));

        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);

        integrationTest(runtime, List.of(Runtime.PHASE.ONE), config);
        assertEquals(PHASE_ONE_TEST_SIZE, SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().count().next().longValue());
    }

    @Test
    public void loopVertexTest() {
        IntStream.range(0, 1000).forEach(i -> {
            testWillTransferVerticesFromGraphAToGraphB();
            cleanup();
            setup();
        });
    }

    @Test
    public void testWillTransferEdgesFromGraphAToGraphB() {

        final Configuration config = ConfigurationUtil.configurationWithOverrides(graphTransferConfig, new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
        }}));

        registerCleanupCallback(() -> {
            SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().drop().iterate();
            LocalParallelStreamRuntime.getInstance(config).close();
        });

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() ->
                TinkerFactory.createClassic().traversal().V().id()));

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.TWO, OneShotSupplier.of(() ->
                TinkerFactory.createClassic().traversal().E().id()));

        final Runtime runtime = LocalParallelStreamRuntime.getInstance(config);

        integrationTest(runtime, List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO), config);

        assertEquals(PHASE_TWO_TEST_SIZE, SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().E().count().next().longValue());
    }
    @Test
    public void loopEdgeTest() {
        IntStream.range(0, 1000).forEach(i -> {
            testWillTransferEdgesFromGraphAToGraphB();
            cleanup();
            setup();
        });
    }

}
