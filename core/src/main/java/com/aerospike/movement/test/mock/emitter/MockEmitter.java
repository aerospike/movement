package com.aerospike.movement.test.mock.emitter;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ErrorHandler;
import com.aerospike.movement.util.core.ErrorUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import org.apache.commons.configuration2.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class MockEmitter extends Loadable implements Emitter {

    private final Configuration config;
    private final ErrorHandler errorHandler;

    public MockEmitter(final Configuration config) {
        super(MockOutput.Config.INSTANCE, config);
        this.config = config;
        this.errorHandler = RuntimeUtil.getErrorHandler(this, config);
    }

    public static void clear() {
    }

    @Override
    public Optional<Object> notify(final Notification n) {
        return Optional.empty();
    }

    @Override
    public void init(final Configuration config) {

    }


    public static class Methods {
        public static final String STREAM = "stream";
        public static final String GET_ALL_PROPERTY_KEYS_FOR_VERTEX_LABEL = "getAllPropertyKeysForVertexLabel";
        public static final String GET_ALL_PROPERTY_KEYS_FOR_EDGE_LABEL = "getAllPropertyKeysForEdgeLabel";
        public static final String PHASES = "phases";
        public static final String CLOSE = "close";
    }

    public static MockEmitter open(Configuration config) {
        return new MockEmitter(config);

    }

    @Override
    public Stream<Emitable> stream(final WorkChunkDriver workChunkDriver, final Runtime.PHASE phase) {
        final WorkChunkDriver wcd = (WorkChunkDriver) MockUtil.onEvent(this.getClass(), Methods.STREAM, this, workChunkDriver, phase)
                .orElseThrow(() ->
                        ErrorUtil.runtimeException("no stream available in mock emitter"));

        return Stream.iterate(wcd.getNext(), wc -> wc.isPresent(), i -> wcd.getNext())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(wc -> IteratorUtils.stream(wc.iterator()))
                .map(id -> new MockEmitable(id, false, config));
    }


    @Override
    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        return (List<String>) MockUtil.onEvent(this.getClass(), Methods.GET_ALL_PROPERTY_KEYS_FOR_VERTEX_LABEL, this, label).orElseThrow(ErrorUtil::unimplemented);
    }

    @Override
    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return (List<String>) MockUtil.onEvent(this.getClass(), Methods.GET_ALL_PROPERTY_KEYS_FOR_EDGE_LABEL, this, label).orElseThrow(ErrorUtil::unimplemented);
    }

    @Override
    public List<Runtime.PHASE> phases() {
        return (List<Runtime.PHASE>) MockUtil.onEvent(this.getClass(), Methods.PHASES, this).orElse(List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO));
    }

    @Override
    public void close() {
        MockUtil.onEvent(this.getClass(), Methods.CLOSE, this);
    }
}
