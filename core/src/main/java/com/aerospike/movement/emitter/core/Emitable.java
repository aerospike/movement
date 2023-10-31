/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.emitter.core;



import com.aerospike.movement.output.core.Output;

import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Emitable {
    static Emitable empty() {
        return new Emitable() {
            @Override
            public Stream<Emitable> emit(Output output) {
                return Stream.empty();
            }

            @Override
            public String type() {
                return "";
            }
        };
    }

    Stream<Emitable> emit(Output output);

//    Stream<Emitable> stream();
    String type();
}
