/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.output.core;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Output extends AutoCloseable {
    static void init(Runtime.PHASE phase, Configuration config) {
//        RuntimeUtil.closeAllInstancesOfLoadable(Output.class);
    }

    //for graph metadata is label of type string
    OutputWriter writer(Class<? extends Emitable> type, String label);

    Emitter reader(Runtime.PHASE phase, Class type, Optional<String> label, Configuration callerConfig);

    Map<String, Object> getMetrics();

    void close();

    void dropStorage();
    Optional<Encoder> getEncoder();

}
