package com.aerospike.graph.generator.emitter.generated.schema.def;

import java.util.List;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphSchema {
    public List<EdgeSchema> edgeTypes;
    public List<VertexSchema> vertexTypes;
    public String entrypointVertexType;
    public double stitchWeight;
    public String stitchType;
}
