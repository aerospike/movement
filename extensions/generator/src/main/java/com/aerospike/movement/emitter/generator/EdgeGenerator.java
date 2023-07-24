package com.aerospike.movement.emitter.generator;


import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.graph.EmittedEdge;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.runtime.core.driver.OutputId;
import com.aerospike.movement.runtime.core.driver.OutputIdDriver;
import com.aerospike.movement.structure.core.EmittedId;
import com.aerospike.movement.structure.core.EmittedIdImpl;
import com.aerospike.movement.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.emitter.generator.schema.def.VertexSchema;
import com.aerospike.movement.util.generator.GeneratorUtil;

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
        output.writeToOutput((EmittedEdge) new GeneratedEdge(this, inV, outV));
        emitted = true;
    }


    public GeneratedEdge emit(final OutputWriter output, final long inV, final long outV) {
        if (!emitted)
            callOutput(output, inV, outV);
        else
            throw new IllegalStateException("Edge already emitted");
        return new GeneratedEdge(this, inV, outV);
    }

    public Stream<Emitable> walk(final Long outVid, final OutputIdDriver outputIdDriver) {
        return Stream.of(new Path(outVid, this, outputIdDriver));
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
            return edgeSchema.label();
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
        private final OutputIdDriver outputIdDriver;
        private final Long outVid;

        public Path(final Long outVid, final EdgeGenerator edgeGenerator, OutputIdDriver outputIdDriver) {
            this.outVid = outVid;
            this.edgeGenerator = edgeGenerator;
            this.outputIdDriver = outputIdDriver;
        }


        public Stream<Emitable> next() {
            final VertexSchema vertexSchema = GeneratorUtil.getSchemaFromVertexName(graphSchema, edgeSchema.inVertex);
            final Optional<OutputId> maybeNext = outputIdDriver.getNext();
            if(maybeNext.isEmpty())
                return Stream.empty();
            Long nextInVid = (Long) maybeNext.get().getId();
            final GeneratedEdge ge = new GeneratedEdge(edgeGenerator, nextInVid, outVid);
            final VertexContext context = new VertexContext(graphSchema,
                    vertexSchema,
                    outputIdDriver,
                    Optional.of(ge));
            nextVertex = new GeneratedVertex((Long) nextInVid, context);
            return Stream.of(nextVertex);
        }

        public Stream<Emitable> oldEmit(final Output output) {
            if (nextVertex == null) { // need to write vertex first
                //Lets look and see if there is a next, tell the stream to look ahead and get the iterator
                final Iterator<Emitable> xi = next().iterator();
                if(!xi.hasNext()) //none availble
                    return Stream.empty();

                //The opposing vertex is available
                final Emitable y = xi.next();

                //write the opposing vertex, then ourselves (the edge)
                return Stream.of(y, this); //@todo check this
            }
            output.writer(EmittedEdge.class, edgeGenerator.edgeSchema.label()).writeToOutput((EmittedEdge) new GeneratedEdge(edgeGenerator, (Long) nextVertex.id, outVid));
            return Stream.empty();
        }

        public Stream<Emitable> newEmit(final Output output) {
            if (nextVertex == null) { // need to write vertex first
                final Stream<Emitable> x = next();
                final Iterator<Emitable> i = x.iterator();
                if (!i.hasNext())
                    return Stream.empty();
                try {
                    final Emitable z = i.next();
                    return Stream.of(z, this); //@todo check this
                } catch (NoSuchElementException nse) {
                    throw new RuntimeException(nse);
                }
            }
            output.writer(EmittedEdge.class, edgeGenerator.edgeSchema.label()).writeToOutput((EmittedEdge) new GeneratedEdge(edgeGenerator, (Long) nextVertex.id, outVid));
            return Stream.empty();
        }

        @Override
        public Stream<Emitable> emit(final Output output) {
            return oldEmit(output);
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


