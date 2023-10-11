package com.aerospike.movement.output.tinkerpop;

import com.aerospike.movement.emitter.generator.Generator;
import com.aerospike.movement.emitter.generator.schema.YAMLParser;
import com.aerospike.movement.runtime.core.driver.impl.GeneratedOutputIdDriver;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.tinkerpop.common.RemoteGraphTraversalProvider;
import com.aerospike.movement.tinkerpop.common.instrumentation.TinkerPopGraphProvider;
import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.IOUtil;
import com.aerospike.movement.util.core.iterator.ConfiguredRangeSupplier;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
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
import java.util.stream.LongStream;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.*;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.THREADS;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

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
        put(ConfigurationBase.Keys.EMITTER, SourceGraph.class.getName());
        put(SourceGraph.Config.Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
        put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, ClassicGraph.class.getName());

        put(ConfigurationBase.Keys.ENCODER, TraversalEncoder.class.getName());
        put(TraversalEncoder.Config.Keys.TRAVERSAL_PROVIDER, SharedEmptyTinkerGraphTraversalProvider.class.getName());
        put(ConfigurationBase.Keys.OUTPUT, TraversalOutput.class.getName());
    }});


    @Test
    public void testWillTransferVerticesFromGraphAToGraphBbyTraversal() {

        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().E().count().next().longValue());
        assertEquals(0L, SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().count().next().longValue());

        final Configuration config = ConfigurationUtil.configurationWithOverrides(graphTransferConfig, new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
            put(THREADS, 1); //TinkerGraph is single threaded
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


    @Test
    @Ignore
    public void testExternalConfig() throws IOException {
        final Long SCALE_FACTOR = 100L;
        final File schemaFile = new File("/home/g/ext_code/movement/scratchpad/ews_original_schema_v3.yaml");

        final Configuration testConfig = ConfigurationUtil.configurationWithOverrides(ConfigurationUtil.load("/home/g/ext_code/movement/scratchpad/ews_demo.properties"),
                new HashMap<>() {{
                    put(YAMLParser.Config.Keys.YAML_FILE_PATH, (String) schemaFile.getAbsolutePath());
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
//        assertEquals(1700L, g.V().count().next().longValue());
//        assertEquals(1600L, g.E().count().next().longValue());
    }


}
