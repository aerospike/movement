package com.aerospike.graph.generator.emitter.generated;

import com.aerospike.graph.generator.emitter.generated.schema.def.EdgeSchema;
import com.aerospike.graph.generator.emitter.generated.schema.def.GraphSchema;
import com.aerospike.graph.generator.structure.Util;

import java.util.Iterator;
import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class EmittedEdgeIterator implements Iterator<Optional<EdgeGenerator>> {

    private final EdgeSchema edgeSchema;
    private final double likelyhood;
    private final int count;
    private final GraphSchema graphSchema;
    private int flips;

    public EmittedEdgeIterator(final EdgeSchema edgeSchema, final GraphSchema graphSchema, final double likelihood, final int count) {
        this.edgeSchema = edgeSchema;
        this.likelyhood = likelihood;
        this.count = count;
        this.flips = 0;
        this.graphSchema = graphSchema;
    }

    @Override
    public boolean hasNext() {
        return flips < count;
    }

    @Override
    public Optional<EdgeGenerator> next() {
        flips++;
        if (Util.coinFlip(likelyhood))
            return Optional.of(new EdgeGenerator(edgeSchema, graphSchema));
        return Optional.empty();
    }
}