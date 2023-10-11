package com.aerospike.movement.emitter.generator.schema.def;

import java.util.Iterator;
import java.util.List;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphSchema {
    public List<EdgeSchema> edgeTypes;
    public List<VertexSchema> vertexTypes;
    public String entrypointVertexType;

    @Override
    public boolean equals(Object o) {
        if (!GraphSchema.class.isAssignableFrom(o.getClass()))
            return false;
        GraphSchema other = (GraphSchema) o;
        for (EdgeSchema e : edgeTypes) {
            final Iterator<EdgeSchema> i = other.edgeTypes.stream()
                    .filter(it -> it.label().equals(e.label())).iterator();
            if (!i.hasNext())
                return false;
            if (!i.next().equals(e))
                return false;
        }
        for (VertexSchema v : vertexTypes) {
            final Iterator<VertexSchema> i = other.vertexTypes.stream()
                    .filter(it -> it.label().equals(v.label())).iterator();
            if (!i.hasNext())
                return false;
            if (!i.next().equals(v))
                return false;
        }
        if (!entrypointVertexType.equals(other.entrypointVertexType))
            return false;
        return true;
    }
}
