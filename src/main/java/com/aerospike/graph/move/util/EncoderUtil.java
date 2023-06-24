package com.aerospike.graph.move.util;

import com.aerospike.graph.move.emitter.EmittedEdge;

public class EncoderUtil {
    public static String getFieldFromEdge(EmittedEdge edge, String field) {
        if (field.equals("~label"))
            return edge.label();
        if (field.equals("~to"))
            return String.valueOf(edge.toId().getId());
        if (field.equals("~from"))
            return String.valueOf(edge.fromId().getId());
        else
            return String.valueOf(edge.propertyValue(field).orElse(""));
    }
}
