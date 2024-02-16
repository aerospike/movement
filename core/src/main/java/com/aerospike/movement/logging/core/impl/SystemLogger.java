/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.logging.core.impl;


import com.aerospike.movement.logging.core.Level;
import com.aerospike.movement.logging.core.Logger;

import java.util.Arrays;
import java.util.Optional;

public class SystemLogger implements Logger {
    private final Optional<Object> context;

    public SystemLogger(final Object context) {
        this.context = Optional.ofNullable(context);
    }

    public SystemLogger() {
        this.context = Optional.empty();
    }

    private static String format(final Level level, final String message, Optional<Object> loggerContext, Object... messageContext) {
        return String.format("[%s][%s] %s", level.name(), loggerContext.orElse("-"), String.format(message, messageContext));
    }

    private void log(Level level, final String message, Object... context) {
        if (context.length > 0) {
            Arrays.stream(context).filter(it -> Throwable.class.isAssignableFrom(it.getClass())).findFirst()
                    .ifPresent(it -> ((Throwable) it).printStackTrace());
        }
        System.out.println(format(level, message, this.context, context));
    }

    @Override
    public void info(final String message, Object... context) {
        System.out.println(format(Level.INFO, message, this.context, context));

    }

    @Override
    public void error(final String message, Object... context) {
        Arrays.stream(context).filter(it -> Throwable.class.isAssignableFrom(it.getClass())).findFirst()
                .ifPresent(it -> ((Throwable) it).printStackTrace());
        System.err.println(format(Level.ERROR, message, this.context, context));
    }

    @Override
    public void debug(final String message, Object... context) {
//        System.out.println(format(Level.DEBUG, message, this.context, context));
    }

    @Override
    public void warn(final String message, Object... context) {
        System.out.println(format(Level.WARN, message, this.context, context));
    }
}
