package com.aerospike.movement.emitter.core.graph;

import com.aerospike.movement.structure.core.EmittedId;

public interface EmittedEdge extends EmitableGraphElement {

    String LABEL = "~label";
    String TO = "~to";
    String FROM = "~from";

    EmittedId fromId();

    EmittedId toId();
    static String getFieldFromEdge(EmittedEdge edge, String field) {
        if (field.equals(LABEL))
            return edge.label();
        if (field.equals(TO))
            return String.valueOf(edge.toId().getId());
        if (field.equals(FROM))
            return String.valueOf(edge.fromId().getId());
        else
            return String.valueOf(edge.propertyValue(field).orElse(""));
    }
}
