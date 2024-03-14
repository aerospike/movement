/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;

public interface GraphProvider extends Provider<Graph, GraphProvider.GraphProviderContext> {

    Exception NO_CONTEXT = new RuntimeException("missing " + Keys.CONTEXT);

    enum GraphProviderContext {
        INPUT, OUTPUT;

        public String toString() {
            return this.name().toLowerCase();
        }

        public static GraphProviderContext fromString(String name) {
            return valueOf(GraphProviderContext.class, name.toUpperCase());
        }

        public static GraphProviderContext fromConfig(Configuration config) {
            try {
                return fromString(Optional.ofNullable(config.getString(Keys.CONTEXT)).orElseThrow(() -> NO_CONTEXT));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    class Keys {
        public static final String INPUT = GraphProviderContext.INPUT.toString().toLowerCase();
        public static final String OUTPUT = GraphProviderContext.OUTPUT.toString().toLowerCase();
        public static final String CONTEXT = "graph.provider.context";
    }
}
