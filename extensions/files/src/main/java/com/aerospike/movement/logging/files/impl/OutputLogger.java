/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.logging.files.impl;

import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.logging.core.Level;
import com.aerospike.movement.logging.core.LogMessage;
import com.aerospike.movement.logging.core.Logger;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.files.SplitFileLineOutput;
import org.apache.commons.configuration2.Configuration;

import java.util.Optional;

public abstract class OutputLogger implements Logger, AutoCloseable {

    protected final Encoder<String> encoder;
    protected final Output output;
    private final Configuration config;

    protected OutputLogger(final Encoder<String> encoder, Output output, final Configuration config) {
        this.encoder = encoder;
        this.output = output;
        this.config = config;
    }

    abstract void log(LogMessage message);

    protected void writeToOutput(LogMessage logMessage) {
        final String meta = encoder.encodeItemMetadata(logMessage)
                .orElseThrow(() -> new RuntimeException("Could not encode header metadata for message: " + logMessage));
        output.writer(LogMessage.class, meta).writeToOutput(Optional.of(logMessage));
    }


    @Override
    public void info(final String message, final Object... context) {
        log(LogMessage.create(this, Level.INFO, message, context));
    }

    @Override
    public void error(final String message, final Object... context) {
        log(LogMessage.create(this, Level.ERROR, message, context));
    }

    @Override
    public void debug(final String message, final Object... context) {
        log(LogMessage.create(this, Level.DEBUG, message, context));
    }

    @Override
    public void warn(final String message, final Object... context) {
        log(LogMessage.create(this, Level.WARN, message, context));
    }
    @Override
    public void close() {
        encoder.close();
        output.close();
    }

}
