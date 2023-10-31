/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.files.csv;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.util.core.error.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.List;
import java.util.Optional;

public abstract class CSVEncoder extends Loadable implements Encoder<String> {
    protected CSVEncoder(final ConfigurationBase configurationMeta, Configuration config) {
        super(configurationMeta, config);
    }

    public static String toCsvLine(final List<String> fields) {
        Optional<String> x = fields.stream().reduce((a, b) -> a + "," + b);
        return x.get();
    }


    @Override
    public String encode(final Emitable item) {
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            return CSVEncoder.toCsvLine(toCsvFields((EmittedEdge) item));
        }
        if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return CSVEncoder.toCsvLine(toCsvFields((EmittedVertex) item));
        }
        throw ErrorUtil.runtimeException("Cannot encode %s", item.getClass().getName());
    }

    protected abstract List<String> toCsvFields(final Emitable item);
}
