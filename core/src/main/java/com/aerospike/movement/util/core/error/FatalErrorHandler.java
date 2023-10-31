/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.error;

import com.aerospike.movement.runtime.core.Handler;

interface FatalErrorHandler extends Handler<Throwable> {
    void fatalErrorAction(final Throwable e, final Object... context);

    @Override
    default void handle(Throwable e, Object... context) {
        this.fatalErrorAction(e, context);
    }
}
