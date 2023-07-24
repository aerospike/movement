package com.aerospike.movement.emitter.generator.schema.def;

import java.util.Iterator;
import java.util.List;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class EdgeSchema {
    public String name;
    public String inVertex;
    public String outVertex;

    public String label() {
        return name;
    }

    public List<PropertySchema> properties;

    @Override
    public boolean equals(Object o) {
        if (!o.getClass().isAssignableFrom(EdgeSchema.class))
            return false;
        EdgeSchema other = (EdgeSchema) o;
        if (!name.equals(other.name))
            return false;
        if (!inVertex.equals(other.inVertex))
            return false;
        if (!outVertex.equals(other.outVertex))
            return false;
        for (PropertySchema p : properties) {
            final Iterator<PropertySchema> i = other.properties.stream().filter(it -> it.name.equals(p.name)).iterator();
            if (!i.hasNext())
                return false;
            if (!i.next().equals(p))
                return false;
        }
        return true;
    }
}
