package com.aerospike.graph.move.emitter.generator;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.output.OutputWriter;
import com.aerospike.graph.move.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.structure.EmittedId;
import com.aerospike.graph.move.structure.EmittedIdImpl;
import com.aerospike.graph.move.util.GeneratorUtil;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

/*
 there is an eager and a passive way of creating an edge
 the eager way of creating an edge is traversal.
 the iterator way walks over the edge and emits it and walks to the next vertex across the edge

 the passive way takes 2 emitted vertex ids and just emits an edge for them
 */

public class EdgeGenerator {
    public final EdgeSchema edgeSchema;
    public final GraphSchema graphSchema;
    private GeneratedVertex nextVertex;
    private boolean emitted = false;

    public EdgeGenerator(final EdgeSchema edgeSchema, final GraphSchema graphSchema) {
        this.graphSchema = graphSchema;
        this.edgeSchema = edgeSchema;
    }

    private void callOutput(final OutputWriter output, final long inV, final long outV) {
        output.writeEdge((EmittedEdge) new GeneratedEdge(this, inV, outV));
        emitted = true;
    }


    public GeneratedEdge emit(final OutputWriter output, final long inV, final long outV) {
        if (!emitted)
            callOutput(output, inV, outV);
        else
            throw new IllegalStateException("Edge already emitted");
        return new GeneratedEdge(this, inV, outV);
    }

    public Stream<Emitable> walk(final Long outVid, final Iterator<Long> idSupplier) {
        return Stream.of(new Path(outVid, this, idSupplier));
    }

    public class GeneratedEdge extends EdgeGenerator implements EmittedEdge {
        private final Long inV;
        private final Long outV;

        public GeneratedEdge(final EdgeGenerator edgeGenerator, final Long inV, final Long outV) {
            super(edgeGenerator.edgeSchema, edgeGenerator.graphSchema);
            this.inV = inV;
            this.outV = outV;
        }

        @Override
        public EmittedId fromId() {
            return new EmittedIdImpl(outV);
        }

        @Override
        public EmittedId toId() {
            return new EmittedIdImpl(inV);
        }

        @Override
        public Stream<String> propertyNames() {
            return edgeSchema.properties.stream().map(p -> p.name);
        }

        @Override
        public Optional<Object> propertyValue(final String name) {
            return Optional.of(EdgeGenerator.getFieldFromEdge(this, name));
        }

        @Override
        public String label() {
            return edgeSchema.label;
        }

        @Override
        public Stream<Emitable> emit(final Output writer) {
            return Stream.empty(); //@todo should not have this here
        }

        @Override
        public Stream<Emitable> stream() {
            throw new IllegalStateException();
        }
    }

    private class Path implements Emitable {
        private final EdgeGenerator edgeGenerator;
        private final Iterator<Long> idSupplier;
        private final Long outVid;

        public Path(final Long outVid, final EdgeGenerator edgeGenerator, final Iterator<Long> idSupplier) {
            this.outVid = outVid;
            this.edgeGenerator = edgeGenerator;
            this.idSupplier = idSupplier;
        }


        public Stream<Emitable> next() {
            final VertexSchema vertexSchema = GeneratorUtil.getSchemaFromVertexName(graphSchema, edgeSchema.inVertex);
            if (!idSupplier.hasNext())
                return Stream.empty();
            Long nextId;
            try {
                nextId = idSupplier.next();
            } catch (NoSuchElementException e) {
                return Stream.empty();
            }
            final GeneratedEdge ge = new GeneratedEdge(edgeGenerator, nextId, outVid);
            final VertexContext context = new VertexContext(graphSchema,
                    vertexSchema,
                    idSupplier,
                    Optional.of(ge));
            if (!idSupplier.hasNext())
                return Stream.empty();
            nextVertex = new GeneratedVertex(idSupplier.next(), context);
            return Stream.of(nextVertex);
        }

        @Override
        public Stream<Emitable> emit(final Output output) {
            if (nextVertex == null) { // need to write vertex first
                final Stream<Emitable> x = next();
                if (!x.iterator().hasNext())
                    return Stream.empty();
                return Stream.of(next().iterator().next(), this); //@todo check this
            }
            output.edgeWriter(edgeGenerator.edgeSchema.label).writeEdge((EmittedEdge) new GeneratedEdge(edgeGenerator, nextVertex.id, outVid));
            return Stream.empty();
        }

        @Override
        public Stream<Emitable> stream() {
            return next();
        }
    }

    private static String getFieldFromEdge(final EmittedEdge edge, final String field) {
        if (field.equals("~label"))
            return edge.label();
        if (field.equals("~to"))
            return String.valueOf(edge.toId().getId());
        if (field.equals("~from"))
            return String.valueOf(edge.fromId().getId());
        else
            return ((GeneratedEdge) edge).edgeSchema.properties.stream()
                    .filter(p -> p.name.equals(field))
                    .map(p -> ValueGenerator.getGenerator(p.valueGenerator).generate(p.valueGenerator.args))
                    .findFirst().orElseThrow(() -> new NoSuchElementException("could not find generator")).toString();
    }
}

