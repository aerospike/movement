package com.aerospike.graph.generator.format.tinkerpop;

import com.aerospike.graph.generator.AbstractGeneratorTest;
import com.aerospike.graph.generator.emitter.Emitable;
import com.aerospike.graph.generator.emitter.generated.GeneratedVertex;
import com.aerospike.graph.generator.emitter.generated.VertexContext;
import com.aerospike.graph.generator.emitter.generated.schema.def.EdgeSchema;
import com.aerospike.graph.generator.emitter.generated.schema.def.GraphSchema;
import com.aerospike.graph.generator.emitter.generated.schema.def.OutEdgeSpec;
import com.aerospike.graph.generator.emitter.generated.schema.def.VertexSchema;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.encoder.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.output.OutputWriter;
import com.aerospike.graph.generator.output.graph.GraphOutput;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;

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
        final Output graphOutput = new GraphOutput(graph, encoder);
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
        final Output graphOutput = new GraphOutput(graph, encoder);
        final GraphSchema graphSchema = testGraphSchema();
        final VertexSchema rootVertexSchema = testVertexSchema();
        final Iterator<Long> idSupplier = LongStream.range(0, 100).iterator();
        final VertexContext vertexContext = new VertexContext(graphSchema, rootVertexSchema, idSupplier);
        final Emitable rootVertex = new GeneratedVertex(true, idSupplier.next(), vertexContext);
        rootVertex.emit(graphOutput);
        final Iterator<Emitable> paths = rootVertex.stream().iterator();
        assertTrue(paths.hasNext());
        IteratorUtils.iterate(RuntimeUtil.walk(IteratorUtils.stream(paths), graphOutput).iterator());
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

}
