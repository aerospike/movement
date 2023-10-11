package com.aerospike.movement.test.mock.emitter;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.test.mock.MockUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.stream.Stream;

public class MockEmitable implements Emitable {

    private final boolean empty;
    private final Object value;


    public MockEmitable(final Object value, final boolean empty, final Configuration config) {
        this.empty = empty;
        this.value = value;
    }

    public static class Methods {
        public static final String EMIT = "emit";
        public static final String STREAM = "stream";
    }

    @Override
    public Stream<Emitable> emit(final Output writer) {
        writer.writer(this.getClass(), "mock").writeToOutput(this);
        return this.empty ? Stream.empty() : (Stream<Emitable>) MockUtil.onEvent(this.getClass(), Methods.EMIT, this, writer).orElseGet(Stream::empty);
    }

    @Override
    public Stream<Emitable> stream() {
        return this.empty ? Stream.empty() : (Stream<Emitable>) MockUtil.onEvent(this.getClass(), Methods.STREAM, this).orElseGet(Stream::empty);
    }

    @Override
    public String toString() {
        return "MockEmitable{" +
                "empty=" + empty +
                '}';
    }
}
