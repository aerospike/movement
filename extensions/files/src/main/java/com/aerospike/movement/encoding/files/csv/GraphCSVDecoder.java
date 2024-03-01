/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.encoding.files.csv;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.structure.core.graph.EmitableGraphElement;
import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.PHASE_OVERRIDE;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphCSVDecoder extends Loadable implements Decoder<String> {


    @Override
    public Optional<Object> notify(final Loadable.Notification n) {
        return Optional.empty();
    }

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

    final Configuration config;

    private final Optional<Runtime.PHASE> override;

    private GraphCSVDecoder(final Configuration config) {
        super(GraphCSVDecoder.Config.INSTANCE, config);
        this.config = config;
        if(config.containsKey(PHASE_OVERRIDE))
            override = Optional.of(Runtime.PHASE.valueOf(config.getString(PHASE_OVERRIDE)));
        else
            override = Optional.empty();

    }

    public static GraphCSVDecoder open(final Configuration config) {
        return new GraphCSVDecoder(config);
    }

    @Override
    public EmitableGraphElement decodeElement(final String encodedElement, final String headerLine, final Runtime.PHASE phase) {
        final Runtime.PHASE decodePhase = override.orElse(phase);
        if (decodePhase.equals(Runtime.PHASE.ONE))
            return (EmitableGraphElement) new CSVVertex(new CSVLine(encodedElement, headerLine));
        else if (decodePhase.equals(Runtime.PHASE.TWO))
            return (EmitableGraphElement) new CSVEdge(new CSVLine(encodedElement, headerLine));
        throw ErrorUtil.unimplemented();
    }


    @Override
    public void onClose() {

    }

    @Override
    public boolean skipEntry(String line) {
        return line.startsWith("~");
    }

}
