/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.runtime;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class CheckedNotThreadSafe {
    private static final String THREADSAFE_FAILURE = "Thread safety check failed";
    private final Set<Long> threadsSeen = new ConcurrentSkipListSet<>();

    protected void checkThreadAssignment() {
        final Long currentId = Thread.currentThread().getId();
        if (threadsSeen.contains(currentId)) {
            throw new RuntimeException(THREADSAFE_FAILURE);
        }

    }

}
