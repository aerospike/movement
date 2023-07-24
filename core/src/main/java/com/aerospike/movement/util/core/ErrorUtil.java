package com.aerospike.movement.util.core;

import com.aerospike.movement.util.core.exception.UnimplementedException;

public class ErrorUtil {

    public static RuntimeException unimplemented() {
        return new UnimplementedException();
    }

    public static RuntimeException runtimeException(final String s, Object... args) {
        return new RuntimeException(String.format(s, args));
    }
}
