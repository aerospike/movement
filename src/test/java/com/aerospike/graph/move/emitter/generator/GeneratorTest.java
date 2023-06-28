package com.aerospike.graph.move.emitter.generator;

import com.aerospike.graph.move.AbstractMovementTest;
import com.aerospike.graph.move.common.tinkerpop.SharedEmptyTinkerGraph;
import com.aerospike.graph.move.common.tinkerpop.instrumentation.TinkerPopGraphProvider;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.generator.schema.SchemaParser;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.encoding.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.move.output.tinkerpop.GraphOutput;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.nio.file.Path;
import java.util.HashMap;

import static junit.framework.TestCase.*;

public class GeneratorTest extends AbstractMovementTest {
    @Test
    public void testGenerate() {
        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 20L);
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
            put(ConfigurationBase.Keys.ENCODER, GraphEncoder.class.getName());
            put(GraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraph.class.getName());
            put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, SharedEmptyTinkerGraph.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, GraphOutput.class.getName());
            put(Generator.Config.Keys.SCHEMA_FILE, newGraphSchemaLocationRelativeToModule());

        }});

        final Graph graph = SharedEmptyTinkerGraph.getInstance();
        graph.traversal().V().drop().iterate();

        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(config);

        runtime.phaseOne().get();
        runtime.close();


        assertEquals(160L, IteratorUtils.count(graph.vertices()));
        assertEquals(140L, IteratorUtils.count(graph.edges()));
        GraphSchema graphSchema = SchemaParser.parse(Path.of(newGraphSchemaLocationRelativeToModule()));
        graphSchema.vertexTypes.stream().forEach(vt -> {
            if (!graph.traversal().V().hasLabel(vt.label).hasNext())
                fail("Vertex type " + vt.label + " not found");
        });
        graphSchema.edgeTypes.stream().forEach(et -> {
            if (!graph.traversal().E().hasLabel(et.label).hasNext())
                fail("Edge type " + et.label + " not found");
        });
    }
}
