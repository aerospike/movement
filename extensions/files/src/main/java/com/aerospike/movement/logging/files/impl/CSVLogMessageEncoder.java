/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.logging.files.impl;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.files.csv.CSVEncoder;
import com.aerospike.movement.logging.core.LogMessage;
import com.aerospike.movement.util.core.error.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CSVLogMessageEncoder extends CSVEncoder {
    private final Configuration config;

    protected CSVLogMessageEncoder(final Configuration config) {
        super(ConfigurationBase.NONE, config);
        this.config = config;
    }

    public static CSVLogMessageEncoder open(Configuration config) {
        return new CSVLogMessageEncoder(config);
    }

    @Override
    public Optional<String> encodeItemMetadata(final Emitable item) {
        return Optional.of(toCsvLine(toCsvFields(item)));
    }

    @Override
    public Map<String, Object> getEncoderMetadata() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public void init(final Configuration config) {

    }

    @Override
    protected List<String> toCsvFields(final Emitable item) {
        LogMessage message = (LogMessage) item;

        return message.getFields();
    }

    @Override
    public void onClose() {

    }
}
