package com.aerospike.movement.emitter.generator;

import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ValueGeneratorConfig {
    public String impl;
    public Map<String, Object> args;

    @Override
    public boolean equals(Object o) {
        if (!ValueGeneratorConfig.class.isAssignableFrom(o.getClass()))
            return false;
        ValueGeneratorConfig other = (ValueGeneratorConfig) o;
        if (!impl.equals(other.impl))
            return false;
        for (Map.Entry<String, Object> e : args.entrySet()) {
            if (!other.args.containsKey(e.getKey()))
                return false;
            if (!other.args.get(e.getKey()).equals(e.getValue()))
                return false;
        }
        return true;
    }
}
