/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.error.exception;

public class UnimplementedException extends RuntimeException {
    public final static String UNIMPLEMENTED = "Unimplemented";

    public UnimplementedException() {
        super(UNIMPLEMENTED);
    }
}
