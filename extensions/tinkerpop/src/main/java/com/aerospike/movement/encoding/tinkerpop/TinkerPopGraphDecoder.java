/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.tinkerpop;

import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.structure.core.EmittedId;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.structure.core.graph.EmittedEdge;
import com.aerospike.movement.structure.core.graph.EmittedVertex;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.tinkerpop.common.TinkerPopGraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;
import java.util.stream.Stream;

import static com.aerospike.movement.emitter.core.Emitter.encodeToOutput;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TinkerPopGraphDecoder extends Loadable implements Decoder<Element> {


    @Override
    public void init(final Configuration config) {

    }

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

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }

    public static final Config CONFIG = new Config();


    public TinkerPopGraphDecoder(final Configuration config) {
        super(Config.INSTANCE, config);
    }

    public static TinkerPopGraphDecoder open(final Configuration openConfig) {
        return new TinkerPopGraphDecoder(openConfig);
    }


    @Override
    public Emitable decodeElement(final Element encodedElement, final String label, final Runtime.PHASE phase) {
        if (Vertex.class.isAssignableFrom(encodedElement.getClass())) {
            return new EmittedVertex() {
                @Override
                public EmittedId id() {
                    return EmittedId.from(encodedElement.id());
                }

                @Override
                public Stream<String> propertyNames() {
                    return encodedElement.keys().stream();
                }

                @Override
                public Optional<Object> propertyValue(String name) {
                    return encodedElement.keys().contains(name) ?
                            Optional.of(encodedElement.property(name)) : Optional.empty();
                }

                @Override
                public String label() {
                    return encodedElement.label();
                }

                @Override
                public Stream<Emitable> stream() {
                    return Stream.empty();
                }

                @Override
                public Stream<Emitable> emit(final Output output) {
                    encodeToOutput(this, output);
                    return stream();
                }
            };
        } else if (Edge.class.isAssignableFrom(encodedElement.getClass())) {
            return new EmittedEdge() {
                @Override
                public EmittedId fromId() {
                    return EmittedId.from(((Edge) encodedElement).outVertex().id());
                }

                @Override
                public EmittedId toId() {
                    return EmittedId.from(((Edge) encodedElement).inVertex().id());
                }

                @Override
                public Stream<String> propertyNames() {
                    return encodedElement.keys().stream();
                }

                @Override
                public Optional<Object> propertyValue(String name) {
                    return encodedElement.keys().contains(name) ?
                            Optional.of(encodedElement.property(name)) : Optional.empty();
                }

                @Override
                public String label() {
                    return encodedElement.label();
                }

                @Override
                public Stream<Emitable> stream() {
                    return Stream.empty();
                }

                @Override
                public Stream<Emitable> emit(Output output) {
                    encodeToOutput(this, output);
                    return stream();
                }
            };
        } else {
            throw new RuntimeException(encodedElement.getClass().getName() + " not supported by " + TinkerPopGraphDecoder.class.getSimpleName());
        }
    }

    @Override
    public void onClose() {

    }

    @Override
    public boolean skipEntry(Element line) {
        return false;
    }


}
