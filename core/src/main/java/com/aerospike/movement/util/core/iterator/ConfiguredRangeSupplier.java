/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.iterator;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

public class ConfiguredRangeSupplier extends OneShotIteratorSupplier {
    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String RANGE_BOTTOM = "workChunkDriver.supplied.range.bottom";
            public static final String RANGE_TOP = "workChunkDriver.supplied.range.top";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.RANGE_TOP, String.valueOf(Long.MAX_VALUE));
        }};
    }

    private ConfiguredRangeSupplier(final IteratorSupplier supplier) {
        super(supplier);
    }

    public static ConfiguredRangeSupplier open(Configuration config) {
        long rangeBottom = Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.RANGE_BOTTOM, config));
        long rangeTop = Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.RANGE_TOP, config));
        return new ConfiguredRangeSupplier(OneShotIteratorSupplier.of(() ->
                PrimitiveIteratorWrap.wrap(LongStream.range(rangeBottom, rangeTop))));
    }
}
