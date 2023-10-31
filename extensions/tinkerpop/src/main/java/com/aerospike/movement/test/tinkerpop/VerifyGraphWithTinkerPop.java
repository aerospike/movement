/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.test.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopGraphEncoder;
import com.aerospike.movement.output.tinkerpop.TinkerPopGraphOutput;
import com.aerospike.movement.tinkerpop.common.TinkerPopGraphProvider;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;

public class VerifyGraphWithTinkerPop {

    public static Configuration baseConfiguration(){
        return new MapConfiguration(new HashMap<>(){{
            put(ConfigurationBase.Keys.ENCODER, TinkerPopGraphEncoder.class.getName());
            put(TinkerPopGraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
            put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, SharedEmptyTinkerGraphGraphProvider.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, TinkerPopGraphOutput.class.getName());
        }});
    }
    public static boolean verify(final Graph graph, final Long edgeCount, final Long vertexCount, final Function<Graph, Optional<Throwable>> check) {
        assert Objects.equals(graph.traversal().E().count().next(), edgeCount);
        assert Objects.equals(graph.traversal().V().count().next(), vertexCount);
        final Optional<Throwable> error = check.apply(graph);
        if (error.isPresent()) {
            throw new RuntimeException(error.get());
        }
        return true;
    }
    public static Map<Object,Long> getDistribution(GraphTraversalSource g){
        return g.V().groupCount().by(both().count()).next();
    }
}
