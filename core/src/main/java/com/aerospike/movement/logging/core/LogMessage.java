/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.logging.core;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.output.core.Output;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class LogMessage implements Emitable {

    private final Logger logger;

    public static class Fields {
        public static final String TIMESTAMP = "timestamp";
        public static final String LEVEL = "level";
        public static final String MESSAGE = "message";
        public static final String CONTEXT_PREFIX = "context.";
    }


    private final String message;
    private final Level level;
    private final Optional<Object[]> context;

    private LogMessage(final Logger logger, final Level level, final String message, Object... context) {
        this.message = message;
        this.level = level;
        this.context = Optional.ofNullable(context);
        this.logger = logger;
    }

    public static LogMessage create(final Logger logger, final Level level, final String message, Object... context) {
        return new LogMessage(logger, level, message, context);
    }


    @Override
    public Stream<Emitable> emit(final Output output) {
        output.writer(LogMessage.class, this.getClass().getName()).writeToOutput(this);
        return stream();
    }

    protected Logger getLogger() {
        return logger;
    }

    public Stream<Emitable> stream() {
        return Stream.empty();
    }

    @Override
    public String type() {
        return LogMessage.class.getSimpleName();
    }

    public List<String> getFields() {
        final List<String> results = new ArrayList<>() {{
            add(Fields.TIMESTAMP);
            add(Fields.LEVEL);
            add(Fields.MESSAGE);
        }};
        if (context.isPresent()) {
            for (int i = 0; i < context.get().length; i++) {
                results.add(Fields.CONTEXT_PREFIX + i);
            }
        }
        return results;
    }
}
