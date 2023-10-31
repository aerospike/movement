/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.util.tinkerpop;

import org.apache.tinkerpop.gremlin.language.grammar.GremlinAntlrToJava;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class GremlinUtil {
    public static Object eval(final String script, final GraphTraversalSource graphTraversalSource) {
        final GremlinAntlrToJava antlr = new GremlinAntlrToJava(graphTraversalSource);
        Object x = GremlinQueryParser.parse(script, antlr);
        return x;
    }

}
