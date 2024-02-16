/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.test.mock.encoder;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MockEncoder<T> extends Loadable implements Encoder<T> {
    public static class Config extends ConfigurationBase {
        public static final MockEncoder.Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigUtil.getKeysFromClass(MockOutput.Config.Keys.class);
        }


        public static class Keys {

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

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
        super(MockEncoder.Config.INSTANCE, config);
        this.config = config;
    }

    public static MockEncoder open(final Configuration config) {
        return new MockEncoder(config);
    }


    @Override
    public Optional<T> encode(final Emitable item) {
        Object x = MockUtil.onEvent(this.getClass(), Methods.ENCODE, this, item).orElseThrow(ErrorUtil::unimplemented);
        return Optional.class.isAssignableFrom(x.getClass()) ?
                (Optional<T>) x : (Optional<T>) Optional.of(x);
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
