/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.error;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.util.core.error.exception.UnimplementedException;

public class ErrorUtil {

    public static RuntimeException unimplemented() {
        return new UnimplementedException();
    }

    public static RuntimeException runtimeException(final String s, Object... args) {
        return new RuntimeException(String.format(s, args));
    }

    public static Throwable cannotEncodeException(final Emitable item) {
        return new RuntimeException("Cannot encode item: " + item.toString());
    }
}
