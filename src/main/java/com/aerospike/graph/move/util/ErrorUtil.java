package com.aerospike.graph.move.util;

public class ErrorUtil {
    public final static String UNIMPLEMENTED = "Unimplemented";

    public static RuntimeException unimplemented() {
        return new RuntimeException(UNIMPLEMENTED);
    }
}
