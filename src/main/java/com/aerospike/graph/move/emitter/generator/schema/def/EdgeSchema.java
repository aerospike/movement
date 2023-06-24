package com.aerospike.graph.move.emitter.generator.schema.def;

import java.util.List;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class EdgeSchema {
    public String name;
    public String inVertex;
    public String outVertex;
    public String label;
    public List<PropertySchema> properties;



}
