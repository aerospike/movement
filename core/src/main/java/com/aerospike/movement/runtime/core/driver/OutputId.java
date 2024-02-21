/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver;

import com.aerospike.movement.structure.core.EmittedId;

public class OutputId implements EmittedId {
    private final Object id;

    private OutputId(final Object id) {
        this.id = id;
    }

    public static OutputId create(final Object wrappedId) {
        return new OutputId(wrappedId);
    }

    @Override
    public Object unwrap() {
        return id;
    }
}
