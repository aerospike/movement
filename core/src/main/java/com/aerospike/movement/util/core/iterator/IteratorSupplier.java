/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.iterator;

import java.util.Iterator;

public interface IteratorSupplier<T> {
    Iterator<T> get();
}
