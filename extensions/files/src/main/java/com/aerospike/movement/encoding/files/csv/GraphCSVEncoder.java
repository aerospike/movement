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
import com.aerospike.movement.structure.core.graph.TypedField;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
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
            return ConfigUtil.getKeysFromClass(Config.Keys.class);
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
            return Optional.of(toCSVHeader(getEdgeHeaderFields(((EmittedEdge) item).label())));
        }
        if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return Optional.of(toCSVHeader(getVertexHeaderFields(((EmittedVertex) item).label())));
        }
        throw ErrorUtil.runtimeException("Cannot encode metadata for %s", item.getClass().getName());
    }


    @Override
    public Map<String, Object> getEncoderMetadata() {
        return Map.of(SplitFileLineOutput.Config.Keys.EXTENSION, "csv");
    }

    @Override
    public void onClose() {

    }

    public List<TypedField> getVertexHeaderFields(final String label) {
        List<TypedField> fields = new ArrayList<>();
        fields.add(new TypedField("~id", false, String.class));
        fields.add(new TypedField("~label", false, String.class));
        ((Emitter) RuntimeUtil.lookup(Emitter.class).get(0)).getAllPropertyKeysForVertexLabel(label).stream().sorted().forEach(fields::add);
        return fields;
    }

    public List<TypedField> getEdgeHeaderFields(final String label) {
        List<TypedField> fields = new ArrayList<>();
        fields.add(new TypedField("~label",false,String.class));
        fields.add(new TypedField("~from",false,String.class));
        fields.add(new TypedField("~to",false,String.class));
        ((Emitter) RuntimeUtil.lookup(Emitter.class).get(0)).getAllPropertyKeysForEdgeLabel(label).stream().sorted().forEach(fields::add);
        return fields;
    }


    public List<String> toCsvFields(final Emitable item) {
        if (Optional.class.isAssignableFrom(item.getClass()))
            throw new RuntimeException("optional");
        if (EmittedEdge.class.isAssignableFrom(item.getClass())) {
            return getEdgeHeaderFields(((EmittedEdge) item).label()).stream()
                    .map(field -> EmittedEdge.getFieldFromEdge(((EmittedEdge) item), field.name))
                    .map(Object::toString)
                    .collect(Collectors.toList());
        } else if (EmittedVertex.class.isAssignableFrom(item.getClass())) {
            return getVertexHeaderFields(((EmittedVertex) item).label()).stream()
                    .map(field -> String.valueOf(((EmittedVertex) item).propertyValue(field.name).orElse("")))
                    .collect(Collectors.toList());
        }
        throw ErrorUtil.runtimeException("Cannot encode %s", item.getClass().getName());
    }
}
