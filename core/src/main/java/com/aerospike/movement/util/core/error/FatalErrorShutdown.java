/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.error;

import com.aerospike.movement.runtime.core.Handler;
import org.apache.commons.configuration2.Configuration;

public class FatalErrorShutdown implements FatalErrorHandler {
    public static Handler<Throwable> open(Configuration config) {
        return new FatalErrorShutdown();
    }

    public void fatalErrorAction(final Throwable e, final Object... context) {
        e.printStackTrace();
        System.exit(1);
    }
}
