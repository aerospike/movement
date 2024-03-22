/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.emitter.core;

import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.structure.core.graph.TypedField;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.stream.Stream;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.DOT;
import static com.aerospike.movement.config.core.ConfigurationBase.Keys.EMITTER;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Emitter {

    static void encodeToOutput(Emitable emitable, Output output) {
        encodeToOutput(Optional.of(emitable),output);
    }
    static void encodeToOutput(Optional<Emitable> emitable, Output output) {
        if(emitable.isEmpty())
            return;
        output.writer(emitable.get().getClass(), emitable.get().type()).writeToOutput(emitable);
    }

    static void init(Runtime.PHASE phase, Configuration config) {
    }

    Stream<Emitable> stream(final WorkChunkDriver workChunkDriver, final Runtime.PHASE phase);

    //@todo remove from interface, use static class lookups across runtime
    List<TypedField> getAllPropertyKeysForVertexLabel(final String label);

    List<TypedField> getAllPropertyKeysForEdgeLabel(final String label);



    List<Runtime.PHASE> phases();


    interface SelfDriving {
        WorkChunkDriver driver(Configuration configuration);
    }

    interface Constrained {
        class Keys {
            public static final String CONSTRAINT = EMITTER + DOT + "constraint";
        }

        List<String> getConstraints(Configuration callerConfig);

    }
}
