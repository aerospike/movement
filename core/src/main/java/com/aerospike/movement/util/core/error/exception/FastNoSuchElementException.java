/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */
package com.aerospike.movement.util.core.error.exception;

import java.util.NoSuchElementException;

/**
 * Retrieve a singleton, fast {@code NoSuchElementException} without a stack trace.
 */
public final class FastNoSuchElementException extends NoSuchElementException {

    private static final long serialVersionUID = 2303108654138257697L;
    private static final FastNoSuchElementException INSTANCE = new FastNoSuchElementException();

    private FastNoSuchElementException() {
    }

    /**
     * Retrieve a singleton, fast {@code NoSuchElementException} without a stack trace.
     */
    public static NoSuchElementException instance() {
        return INSTANCE;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

}
