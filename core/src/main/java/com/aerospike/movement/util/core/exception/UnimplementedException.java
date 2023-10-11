package com.aerospike.movement.util.core.exception;

public class UnimplementedException extends RuntimeException {
    public final static String UNIMPLEMENTED = "Unimplemented";

    public UnimplementedException() {
        super(UNIMPLEMENTED);
    }
}
