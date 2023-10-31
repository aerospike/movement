/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.files.csv;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.output.files.SplitFileLineOutput;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.error.ErrorUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphCSVEncoder extends CSVEncoder {
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
            public static final String HEADER = "header";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

    final Configuration config;


    public static Encoder<String> open(final Configuration config) {
        return new GraphCSVEncoder(config);
    }


    protected GraphCSVEncoder(final Configuration config) {
        super(Config.INSTANCE, config);
        this.config = config;
    }

    @Override
    public void init(final Configuration config) {

    }


    @Override
    public Optional<String> encodeItemMetadata(final Emitable item) {
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            return Optional.of(toCsvLine(getEdgeHeaderFields(((EmittedEdge) item).label())));
        }
        if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return Optional.of(toCsvLine(getVertexHeaderFields(((EmittedVertex) item).label())));
        }
        throw ErrorUtil.runtimeException("Cannot encode metadata for %s", item.getClass().getName());
    }


    @Override
    public Map<String, Object> getEncoderMetadata() {
        return Map.of(SplitFileLineOutput.Config.Keys.EXTENSION,"csv");
    }

    @Override
    public void close() {

    }

    public List<String> getVertexHeaderFields(final String label) {
        List<String> fields = new ArrayList<>();
        fields.add("~id");
        fields.add("~label");
        ((Emitter) RuntimeUtil.lookupOrLoad(Emitter.class, config)).getAllPropertyKeysForVertexLabel(label).stream().sorted().forEach(fields::add);
        return fields;
    }

    public List<String> getEdgeHeaderFields(final String label) {
        List<String> fields = new ArrayList<>();
        fields.add("~label");
        fields.add("~from");
        fields.add("~to");
        ((Emitter) RuntimeUtil.lookupOrLoad(Emitter.class, config)).getAllPropertyKeysForEdgeLabel(label).stream().sorted().forEach(fields::add);
        return fields;
    }


    public List<String> toCsvFields(final Emitable item) {
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            return getEdgeHeaderFields(((EmittedEdge) item).label()).stream()
                    .map(f -> EmittedEdge.getFieldFromEdge(((EmittedEdge) item), f))
                    .collect(Collectors.toList());
        } else if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return getVertexHeaderFields(((EmittedVertex) item).label()).stream()
                    .map(f -> String.valueOf(((EmittedVertex) item).propertyValue(f).orElse("")))
                    .collect(Collectors.toList());
        }
        throw ErrorUtil.runtimeException("Cannot encode %s", item.getClass().getName());
    }
}
