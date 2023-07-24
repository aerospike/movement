package com.aerospike.movement.emitter.generator.schema.def;

import com.aerospike.movement.emitter.generator.ValueGeneratorConfig;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class PropertySchema {
    public String name;
    public String type;
    public double likelihood;
    public ValueGeneratorConfig valueGenerator;

    @Override
    public boolean equals(Object o) {
        if (!PropertySchema.class.isAssignableFrom(o.getClass()))
            return false;
        PropertySchema other = (PropertySchema) o;
        if (!name.equals(other.name))
            return false;
        if (!type.equals(other.type))
            return false;
        if (likelihood != other.likelihood)
            return false;
        if (!valueGenerator.equals(other.valueGenerator))
            return false;
        return true;
    }
}
