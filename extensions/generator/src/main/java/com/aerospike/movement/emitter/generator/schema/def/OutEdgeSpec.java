package com.aerospike.movement.emitter.generator.schema.def;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class OutEdgeSpec {
    public String name;
    public Double likelihood;
    public Integer chancesToCreate;
    public Integer chancesToJoin = 0;


    public EdgeSchema toEdgeSchema(GraphSchema schema) {
        return schema.edgeTypes.stream()
                .filter(edgeSchema -> edgeSchema.name.equals(name)).findFirst()
                .orElseThrow(() -> new RuntimeException("No edge type found for " + name));
    }

    @Override
    public boolean equals(Object o){
        if (!OutEdgeSpec.class.isAssignableFrom(o.getClass()))
            return false;
        OutEdgeSpec other = (OutEdgeSpec) o;
        if (!name.equals(other.name))
            return false;
        if (!likelihood.equals(other.likelihood))
            return false;
        if (!chancesToCreate.equals(other.chancesToCreate))
            return false;
        if (!chancesToJoin.equals(other.chancesToJoin))
            return false;
        return true;
    }
}
