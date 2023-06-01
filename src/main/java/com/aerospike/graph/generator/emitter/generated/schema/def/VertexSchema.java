package com.aerospike.graph.generator.emitter.generated.schema.def;

import java.util.List;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class VertexSchema {
    public String name;
    public List<OutEdgeSpec> outEdges;
    public String label;
    public List<PropertySchema> properties;
}
