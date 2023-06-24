package com.aerospike.graph.move.emitter.generator;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.emitter.generator.schema.def.OutEdgeSpec;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.output.OutputWriter;
import com.aerospike.graph.move.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.graph.move.process.ValueGenerator;
import com.aerospike.graph.move.structure.EmittedId;
import com.aerospike.graph.move.structure.Util;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * you create a root vertex, then iterate over it
 * the pattern is you emit it, then call next() on it
 * from the root vertex that will yield an iterator of edges
 * you call emit on each of those, then next.
 * They will return iterators of vertices, and the pattern repeats until
 * the composite iterator chain returns no elements
 * <p>
 * when you generate a new dataset, you specify the number of root verticies
 * ids 0 -> rootVertexCount are root verticies
 * when stitching later you can randomly pick from this range
 */

public class GeneratedVertex implements Emitable, EmittedVertex {
    private final boolean root;
    public final VertexContext context;
    private AtomicBoolean emitted = new AtomicBoolean(false);
    public final long id;

    public GeneratedVertex(final boolean root,
                           final long id,
                           final VertexContext vertexContext) {
        this.root = root;
        this.id = id;
        this.context = vertexContext;
    }

    public EmittedId id() {
        return new GeneratedVertexId(this.id);
    }

    public GeneratedVertex(final long id, final VertexContext vertexContext) {
        this(false, id, vertexContext);
    }

    /*
    it would be nice to be able to remember the generated out edges on the vertex
    we need this information later when stitching
     */


    public Stream<Emitable> emit(Output output) {
        if (!emitted.getAndSet(true))
            output.vertexWriter(this.context.vertexSchema.label).writeVertex(this);
        return stream();
    }

    @Override
    public Stream<String> propertyNames() {
        return context.vertexSchema.properties.stream().map(p -> p.name);
    }

    @Override
    public Optional<Object> propertyValue(final String name) {
        return Optional.of(getFieldFromVertex(this, name));
    }

    @Override
    public String label() {
        return getFieldFromVertex(this, "~label");
    }

    @Override
    public Stream<Emitable> stream() {
        return paths();
    }

    public Stream<EdgeGenerator> postProcess(Stream<GeneratedVertex> otherNodes, OutputWriter output, StitchMemory stitchMemory) {
        //in this context otherNodes are root nodes
        return otherNodes.flatMap(
                otherRoot ->
                        this.stitch(otherRoot, output, stitchMemory));
    }

    private Stream<EdgeGenerator> stitch(GeneratedVertex otherRoot, OutputWriter output, StitchMemory memory) {
        if (Util.coinFlip(context.graphSchema.stitchWeight)) {
            //@todo suppost stitching on an inType and an outType, ie person owned car
            final Object pointA = memory.outV(otherRoot, context.graphSchema.stitchType).iterator().next().id;
            final Object pointB = memory.outV(this, context.graphSchema.stitchType).iterator().next().id;

            //TODO: we should have n chances of stitching and return each edge created
            final EdgeGenerator.GeneratedEdge x =
                    new EdgeGenerator(Util.getStitchSchema(context.graphSchema), context.graphSchema)
                            .emit(output, (Long) pointA, (Long) pointB);
            return Stream.of(x);
        }
        return Stream.empty();
    }

    public Stream<Emitable> paths() {
        List<OutEdgeSpec> outEdges = context.vertexSchema.outEdges;
        try {
            return IteratorUtils.stream(new Paths(this, outEdges.stream()
                    .flatMap(outEdgeSpec -> {
                        final EdgeSchema edgeSchema;
                        final Double likelihood;
                        final Integer chancesToCreate;
                        try {
                            edgeSchema = Util.getSchemaFromEdgeName(context.graphSchema, outEdgeSpec.name);
                            likelihood = outEdgeSpec.likelihood;
                            chancesToCreate = outEdgeSpec.chancesToCreate;
                        } catch (NullPointerException e) {
                            throw new RuntimeException(e);
                        }
                        return IteratorUtils.stream(
                                new EmittedEdgeIterator(
                                        edgeSchema,
                                        context.graphSchema,
                                        likelihood,
                                        chancesToCreate));
                    }).filter(it -> it.isPresent()).map(it -> it.get())
                    .flatMap(edgeGenerator -> edgeGenerator.walk(this.id, context.idSupplier))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private class Paths implements Iterator<Emitable> {
        private final GeneratedVertex vertex;
        private final Iterator<Emitable> iterator;

        public Paths(GeneratedVertex generatedVertex, Stream<Emitable> stream) {
            this.vertex = generatedVertex;
            this.iterator = stream.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Emitable next() {
            return iterator.next();
        }
    }

    public static class GeneratedVertexId implements EmittedId {
        private final Object id;

        public GeneratedVertexId(final Object id) {
            this.id = id;
        }

        @Override
        public Object getId() {
            return id;
        }
    }

    //@todo should return Object
    private static String getFieldFromVertex(final GeneratedVertex vertex, final String field) {
        if (field.equals("~id"))
            return String.valueOf(vertex.id);
        if (field.equals("~label"))
            return vertex.context.vertexSchema.label;
        else
            return vertex.context.vertexSchema.properties.stream()
                    .filter(p -> p.name.equals(field))
                    .map(p ->
                            ValueGenerator.getGenerator(p.valueGenerator).generate(p.valueGenerator.args))
                    .findFirst().orElseThrow(() -> new NoSuchElementException("could not find generator")).toString();
    }
}
