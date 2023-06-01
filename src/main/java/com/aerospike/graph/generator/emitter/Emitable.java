package com.aerospike.graph.generator.emitter;

import com.aerospike.graph.generator.output.Output;

import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Emitable {
    Stream<Emitable> emit(Output writer);

    Stream<Emitable> stream();
}
