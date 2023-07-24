package com.aerospike.movement.test.mock.encoder;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Map;
import java.util.Optional;

public class MockEncoder<T> extends Loadable implements Encoder<T> {

    private final Configuration config;

    @Override
    public Optional<Object> notify(final Notification n) {
        return Optional.empty();
    }

    @Override
    public void init(final Configuration config) {

    }

    public static class Methods {
        public static final String ENCODE = "encode";
        public static final String CLOSE = "close";
        public static final String GET_EXTENSION = "getExtension";
        public static final String ENCODE_ITEM_METADATA = "encodeItemMetadata";
        public static final String GET_ENCODER_METADATA = "getEncoderMetadata";

    }

    public MockEncoder(final Configuration config) {
        super(MockOutput.Config.INSTANCE, config);
        this.config = config;
    }

    public static MockEncoder open(final Configuration config) {
        return new MockEncoder(config);
    }


    @Override
    public T encode(final Emitable item) {
        return (T) MockUtil.onEvent(this.getClass(), Methods.ENCODE, this, item).orElseThrow(ErrorUtil::unimplemented);
    }


    @Override
    public Optional<T> encodeItemMetadata(final Emitable item) {
        final Object x = MockUtil.onEvent(this.getClass(), Methods.ENCODE_ITEM_METADATA, this, item).orElseThrow(ErrorUtil::unimplemented);
        return (Optional<T>) x;
    }

    @Override
    public Map<String, Object> getEncoderMetadata() {
        return (Map<String, Object>) MockUtil.onEvent(this.getClass(), Methods.GET_ENCODER_METADATA, this).orElseThrow(ErrorUtil::unimplemented);
    }

    @Override
    public void close() {
        MockUtil.onEvent(this.getClass(), Methods.CLOSE, this);
    }
}
