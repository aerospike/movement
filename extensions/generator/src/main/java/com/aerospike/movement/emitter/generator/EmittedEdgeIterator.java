package com.aerospike.movement.emitter.generator;

import com.aerospike.movement.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.util.generator.GeneratorUtil;

import java.util.Iterator;
import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class EmittedEdgeIterator implements Iterator<Optional<EdgeGenerator>> {

    private final EdgeSchema edgeSchema;
    private final double LIKELIHOOD;
    private final int count;
    private final GraphSchema graphSchema;
    private int flips;

    public EmittedEdgeIterator(final EdgeSchema edgeSchema, final GraphSchema graphSchema, final double likelihood, final int count) {
        this.edgeSchema = edgeSchema;
        this.LIKELIHOOD = likelihood;
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
        if (GeneratorUtil.coinFlip(LIKELIHOOD))
            return Optional.of(new EdgeGenerator(edgeSchema, graphSchema));
        return Optional.empty();
    }
}
