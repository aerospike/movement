package com.aerospike.graph.move.format.tinkerpop;

import com.aerospike.graph.move.AbstractGeneratorTest;
import com.aerospike.graph.move.common.tinkerpop.instrumentation.TinkerPopGraphProvider;
import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.generator.GeneratedVertex;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.emitter.generator.StitchProcess;
import com.aerospike.graph.move.emitter.generator.VertexContext;
import com.aerospike.graph.move.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.OutEdgeSpec;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.encoding.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.output.OutputWriter;
import com.aerospike.graph.move.output.file.DirectoryOutput;
import com.aerospike.graph.move.output.tinkerpop.GraphOutput;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.RuntimeUtil;
import junit.framework.TestCase;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;

import static com.aerospike.graph.move.config.ConfigurationBase.configurationToPropertiesFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestTinkerPopFormat extends AbstractGeneratorTest {

    @Test
    public void testWriteVertex() {
        Graph graph = TinkerGraph.open();
        final Encoder<Element> encoder = new GraphEncoder(graph);
        final Output graphOutput = new GraphOutput((GraphEncoder) encoder);
        final GraphSchema graphSchema = testGraphSchema();
        final VertexSchema vertexSchema = testVertexSchema();
        final VertexContext vertexContext = new VertexContext(graphSchema, vertexSchema, IteratorUtils.of(1L));
        GeneratedVertex vertex = new GeneratedVertex(1, vertexContext);
        ((OutputWriter) graphOutput).writeVertex(vertex);
        assertTrue(graph.traversal().V().hasLabel(vertexSchema.label).hasNext());
        final Vertex testV = graph.traversal().V().hasLabel(vertexSchema.label).next();
        assertTrue(testV.property(vertexSchema.properties.get(0).name).isPresent());
    }


    @Test
    public void testIterateRootVertex() {
        Graph graph = TinkerGraph.open();
        final Encoder<Element> encoder = new GraphEncoder(graph);
        final Output graphOutput = new GraphOutput((GraphEncoder) encoder);
        final GraphSchema graphSchema = testGraphSchema();
        final VertexSchema rootVertexSchema = testVertexSchema();
        final Iterator<Long> idSupplier = LongStream.range(0, 100).iterator();
        final VertexContext vertexContext = new VertexContext(graphSchema, rootVertexSchema, idSupplier);
        final Emitable rootVertex = new GeneratedVertex(true, idSupplier.next(), vertexContext);
        rootVertex.emit(graphOutput);
        final Iterator<Emitable> paths = rootVertex.stream().iterator();
        assertTrue(paths.hasNext());
        IteratorUtils.iterate(RuntimeUtil.walk(IteratorUtils.stream(paths), graphOutput));
        final List<Vertex> all = IteratorUtils.list(graph.vertices());
        assertTrue(all.size() > 1);
        assertTrue(graph.traversal().V().hasLabel(rootVertexSchema.label).hasNext());
        final Vertex testV = graph.traversal().V().hasLabel(rootVertexSchema.label).next();
        assertTrue(testV.property(rootVertexSchema.properties.get(0).name).isPresent());
        assertEquals((Long) 5L, (Long) graph.traversal().V().hasLabel(rootVertexSchema.label).outE().count().next());
        final Edge anEdge = graph.traversal().V().hasLabel(rootVertexSchema.label).outE().next();
        final OutEdgeSpec spec = rootVertexSchema.outEdges.stream().filter(outEdgeSpec ->
                outEdgeSpec.name.toLowerCase().equals(anEdge.label())).findFirst().get();
        final EdgeSchema edgeSchema = graphSchema.edgeTypes.stream().filter(edgeType -> edgeType.name.equals(spec.name)).findFirst().get();
        edgeSchema.properties.stream().map(property -> property.name).forEach(name -> assertTrue(anEdge.property(name).isPresent()));
    }

    private static class FireflyConfigurationKeys {
        public static final String AEROSPIKE_HOST = "aerospike.client.host";
        public static final String AEROSPIKE_PORT = "aerospike.client.port";
        public static final String FIREFLY_DATA_MODEL = "aerospike.graph.data.model";
        public static final String AEROSPIKE_NAMESPACE = "aerospike.client.namespace";
    }

    @Test
    @Ignore
    public void testExternalGraphProviderIntegration() {
        final String FIREFLY_GRAPH_CLASS = "com.aerospike.firefly.structure.FireflyGraph";
        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(Generator.Config.Keys.SCHEMA_FILE, testGraphSchemaLocationRelativeToModule());
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 100L);
            put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
            put(DirectoryOutput.Config.Keys.ENCODER, GraphEncoder.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, GraphOutput.class.getName());
            put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, FIREFLY_GRAPH_CLASS);
            put(TinkerPopGraphProvider.Config.Keys.CACHE,"false");
            put(FireflyConfigurationKeys.AEROSPIKE_HOST, "localhost");
            put(FireflyConfigurationKeys.AEROSPIKE_PORT, 3000);
            put(FireflyConfigurationKeys.AEROSPIKE_NAMESPACE, "test");
            put(FireflyConfigurationKeys.FIREFLY_DATA_MODEL, "packed");
        }});
        System.out.println(configurationToPropertiesFormat(config));
        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(config);
        final Graph fireflyGraph = (Graph) RuntimeUtil.openClassRef(FIREFLY_GRAPH_CLASS, config);
        fireflyGraph.traversal().V().drop().iterate();

        runtime.initialPhase().get();
        runtime.completionPhase().get();


        // The correct number of verticies have moved from the TinkerGraph to the CSV files
        TestCase.assertEquals((Long) 600L, (Long) runtime.getOutputVertexMetrics().stream().reduce(0L, (a, b) -> a + b));
        // The correct number of edges have moved from the csv files to the FireflyGraph
        TestCase.assertEquals((Long) fireflyGraph.traversal().E().count().next(), runtime.getOutputEdgeMetrics().stream().reduce(0L, (a, b) -> a + b));
        final VertexSchema rootVertexSchema = schema.vertexTypes.stream()
                .filter(it ->
                        it.label.equals(schema.entrypointVertexType)).findFirst().get();
        final String labelFromSchema = rootVertexSchema.label;

        final List<String> keysForLabel = fireflyGraph.traversal().V().hasLabel(labelFromSchema).properties().key().dedup().toList();
        TestCase.assertEquals((int) schema.vertexTypes.stream()
                        .filter(it -> it.label.equals(labelFromSchema))
                        .flatMap(it -> it.properties.stream())
                        .map(it -> it.name).count(),
                keysForLabel.size());


    }


}
