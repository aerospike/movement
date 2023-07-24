package com.aerospike.movement.util.core;


import com.aerospike.movement.emitter.core.Emitable;

public class EncoderUtil {
    public static Throwable cannotEncodeException(final Emitable item) {
        return new RuntimeException("Cannot encode item: " + item.toString());
    }
}
