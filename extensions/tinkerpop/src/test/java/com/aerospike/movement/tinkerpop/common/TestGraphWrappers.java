/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import com.aerospike.movement.tinkerpop.common.instrumentation.impl.CachedGraph;
import com.aerospike.movement.tinkerpop.common.instrumentation.impl.ThrottledGraph;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//@todo dev-next branch
public class TestGraphWrappers {
    final static class Tokens {
        public static final String PRECEDES = "precedes";
        public static final String SUCCEEDS = "succeeds";

        public static final String NUMBER = "number";
        public static final String VALUE = "value";
        public static final String ZERO = "zero";
        public static final String ONE = "one";
        public static final String TWO = "two";
        public static final String THREE = "three";
        public static final String FOUR = "four";
        public static final String FIVE = "five";
        public static final String SIX = "six";
        public static final String SEVEN = "seven";
        public static final String EIGHT = "eight";
        public static final String NINE = "nine";
    }
//    Ring.of([one,two,three...]))
//      .fullyConnected()

    //    Ring.of([one,two,three...]))
//      .neighborConnected()
    public void setupNumberGraph(final GraphTraversalSource g) {
        final Vertex one = g
                .addV(Tokens.NUMBER)
                .property(T.id, Tokens.ONE)
                .property(Tokens.VALUE, 1L)
                .next();
        final Vertex two = g
                .addV(Tokens.NUMBER)
                .property(T.id, Tokens.TWO)
                .property(Tokens.VALUE, 2L)
                .next();
        g.V(one).addE(Tokens.PRECEDES).to(two).next();
        g.V(two).addE(Tokens.SUCCEEDS).to(one).next();
        assertEquals(2L, g.V().toList().size());
    }

    public void numberGraphTests(final GraphTraversalSource g) {
        setupNumberGraph(g);
        final Vertex oneV = g.V().has(Tokens.VALUE, 1L).next();
        assertEquals((Long) 1L, (Long) oneV.properties(Tokens.VALUE).next().value());
        final Vertex twoV = g.V(oneV).out(Tokens.PRECEDES).next();
        assertEquals((Long) 2L, (Long) twoV.properties(Tokens.VALUE).next().value());
        assertEquals(Tokens.ONE, g.V(twoV).out(Tokens.SUCCEEDS).next().id());
    }

    public void asciiTest(final Graph graph) {
        final String ASCII = "ascii";

        final Vertex a = graph.addVertex();
        assertTrue(graph.traversal().V(a).hasNext());
        final Vertex b = graph.addVertex();
        assertTrue(graph.traversal().V(b).hasNext());
        final Edge e = a.addEdge("aToB", b);
        assertTrue(graph.traversal().V(a).out().hasNext());

        graph.traversal().V(a).property(ASCII, 97).next();
        graph.traversal().V(b).property(ASCII, 98).next();


        assertTrue(graph.traversal().V().has(ASCII, 98).in().hasNext());
        assertEquals(97, graph.traversal().V().has(ASCII, 98).in().next().properties(ASCII).next().value());
    }

    @Test
    public void testGraphTest() {
        Graph tinkerGraph = TinkerGraph.open();
        asciiTest(tinkerGraph);
    }

    @Test
    public void testSharedEmptyTinkerGraphProvider() {
        final Graph sharedEmptyTinkerGraph = SharedEmptyTinkerGraphGraphProvider.open(ConfigUtil.empty()).getProvided(GraphProvider.GraphProviderContext.OUTPUT);
        asciiTest(sharedEmptyTinkerGraph);
        final Graph sharedEmptyTinkerGraphRefTwo = SharedEmptyTinkerGraphGraphProvider.open(ConfigUtil.empty()).getProvided(GraphProvider.GraphProviderContext.OUTPUT);
        assertEquals(2L, sharedEmptyTinkerGraphRefTwo.traversal().V().count().next().longValue());
        assertEquals(1L, sharedEmptyTinkerGraphRefTwo.traversal().E().count().next().longValue());
    }


    @Before
    public void clearTestGraph(){
        SharedEmptyTinkerGraphGraphProvider.getGraphInstance().traversal().V().drop().iterate();
    }
    @Test
    public void testCachedGraph() {
        asciiTest(CachedGraph.open(new MapConfiguration(new HashMap<>() {{
            put(GraphProvider.Keys.CONTEXT,GraphProvider.GraphProviderContext.OUTPUT.toString());
            put(CachedGraph.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
        }})));
    }

    @Test
    public void testThrottledGraph() {
        final long delayTime = 200;
        Graph throttledGraph = ThrottledGraph.open(new MapConfiguration(new HashMap<>() {{
            put(GraphProvider.Keys.CONTEXT,GraphProvider.Keys.OUTPUT);
            put(ThrottledGraph.Config.Keys.GRAPH_PROVIDER_IMPL, SharedEmptyTinkerGraphGraphProvider.class.getName());
            put(ThrottledGraph.Config.Keys.DELAY_TIME, delayTime); //delay each operation .5 seconds;
        }}));
        final long startTime = System.nanoTime();
        Vertex a = throttledGraph.addVertex();
        Vertex b = throttledGraph.addVertex();
        List<Vertex> vertexList = IteratorUtils.list(throttledGraph.vertices());
        final long stopTime = System.nanoTime();
        assertEquals(2, vertexList.size());
        assertTrue(stopTime - startTime >= delayTime * 3);
    }

}
