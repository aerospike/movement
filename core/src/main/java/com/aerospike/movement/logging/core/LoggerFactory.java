/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.logging.core;

import com.aerospike.movement.util.core.runtime.RuntimeUtil;

import java.util.Optional;

public class LoggerFactory {
    final private Optional<Object> context;

    private LoggerFactory(final Object context) {
        this.context = Optional.of(context);
    }

    private LoggerFactory() {
        this.context = Optional.empty();
    }

    public static Logger withContext(Object context) {
        return RuntimeUtil.getLogger(context);
    }
}
