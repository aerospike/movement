/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver;

import com.aerospike.movement.structure.core.EmittedId;

public class WorkItem implements EmittedId {
    protected final Object id;

    public WorkItem(final Object id) {
        this.id = id;
    }

    @Override
    public Object unwrap() {
        return id;
    }
}
