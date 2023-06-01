package com.aerospike.graph.generator.encoder.format.tinkerpop;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.util.EncoderUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;



/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphEncoder extends Encoder<Element> {
    private final Graph graph;

    public GraphEncoder(final Graph graph) {
        this.graph = graph;
    }

    public static GraphEncoder open(final Configuration configuration) {
        return new GraphEncoder(TinkerGraph.open());
    }

    @Override
    public Element encodeEdge(final EmittedEdge edge) {
        final List<Object> args = edge.propertyNames().flatMap(name -> {
            final ArrayList<Object> results = new ArrayList<Object>();
            results.add(name);
            results.add(edge.propertyValue(name));
            return results.stream();
        }).collect(Collectors.toList());
        final Vertex inV = graph.vertices(edge.toId().getId()).next();
        final Vertex outV = graph.vertices(edge.fromId().getId()).next();
        return outV.addEdge(EncoderUtil.getFieldFromEdge(edge, "~label"), inV, args.toArray());
    }


    @Override
    public Element encodeVertex(final EmittedVertex vertex) {
        final List<Object> args = vertex.propertyNames().flatMap(name -> {
            final ArrayList<Object> results = new ArrayList<Object>();
            results.add(name);
            results.add(vertex.propertyValue(name));
            return results.stream();
        }).collect(Collectors.toList());

        args.addAll(0, new ArrayList() {{
            add(T.id);
            add(vertex.id().getId());
            add(T.label);
            add(vertex.label());
        }});
        Vertex x;
        try {
            x = graph.addVertex(args.toArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return x;
    }

    @Override
    public Element encodeVertexMetadata(final EmittedVertex vertex) {
        return null;
    }

    @Override
    public Element encodeEdgeMetadata(final EmittedEdge edge) {
        return null;
    }

    @Override
    public Element encodeVertexMetadata(final String label) {
        return null;
    }

    @Override
    public Element encodeEdgeMetadata(final String label) {
        return null;
    }

    @Override
    public String getExtension() {
        return "";
    }

    @Override
    public void close() {
        try {
            graph.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
