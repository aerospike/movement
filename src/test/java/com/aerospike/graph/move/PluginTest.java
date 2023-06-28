package com.aerospike.graph.move;

import com.aerospike.graph.move.common.tinkerpop.PluginServiceFactory;
import com.aerospike.graph.move.common.tinkerpop.SharedEmptyTinkerGraph;
import com.aerospike.graph.move.common.tinkerpop.instrumentation.TinkerPopGraphProvider;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.encoding.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.move.output.tinkerpop.GraphOutput;
import com.aerospike.graph.move.process.operations.Generate;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import java.util.HashMap;

import static com.aerospike.graph.move.common.tinkerpop.PluginServiceFactory.OPERATION;
import static junit.framework.TestCase.assertEquals;

public class PluginTest extends AbstractMovementTest {
    @Test
    public void testPluginGenerateToGraph() {
        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(CLI.TEST_MODE, true);
            put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1); //TinkerGraph is not threadsafe
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 2000L);
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
            put(Generator.Config.Keys.SCHEMA_FILE, newGraphSchemaLocationRelativeToModule());


            put(ConfigurationBase.Keys.ENCODER, GraphEncoder.class.getName());
            put(GraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraph.class.getName());
            put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, SharedEmptyTinkerGraph.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, GraphOutput.class.getName());
        }});

        Graph graph = SharedEmptyTinkerGraph.getInstance();
        GraphTraversalSource g = graph.traversal();
        graph.traversal().V().drop().iterate();

        graph.getServiceRegistry().registerService(PluginServiceFactory.create(config, Plugin.class.getName(), graph));

        g
                .call(Plugin.NAME)
                .with(OPERATION, Generate.class.getSimpleName())
                .iterate();


        assertEquals(160, graph.traversal().V().count().next().intValue());
        assertEquals(140, graph.traversal().E().count().next().intValue());
    }

}
