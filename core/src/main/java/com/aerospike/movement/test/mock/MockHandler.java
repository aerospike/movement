/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.test.mock;

import java.util.Optional;

public interface MockHandler  {
    Optional<Object> handleEvent(final Object object, final Object... args);

}
