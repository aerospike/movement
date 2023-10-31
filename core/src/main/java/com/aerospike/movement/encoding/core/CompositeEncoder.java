/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.encoding.core;

import com.aerospike.movement.emitter.core.Emitable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CompositeEncoder<T> implements Encoder<T>{
    private final List<Encoder> encoders;

    public CompositeEncoder(List<Encoder> list) {
        this.encoders = list;
    }

    public static <T> CompositeEncoder<T> of(Encoder... encoders){
        return new CompositeEncoder<>(Arrays.asList(encoders));
    }
    @Override
    public T encode(Emitable item) {
        return null;
    }

    @Override
    public Optional<T> encodeItemMetadata(Emitable item) {
        return Optional.empty();
    }

    @Override
    public Map<String, Object> getEncoderMetadata() {
        return null;
    }

    @Override
    public void close() {

    }
}
