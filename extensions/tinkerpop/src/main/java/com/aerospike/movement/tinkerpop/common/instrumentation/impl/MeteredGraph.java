/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common.instrumentation.impl;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.output.Metered;
import com.aerospike.movement.tinkerpop.common.instrumentation.InstrumentedGraph;
import com.aerospike.movement.tinkerpop.common.instrumentation.WrappedGraph;

import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

public class MeteredGraph extends InstrumentedGraph implements Metered {
    private int backlogLimit;

    public MeteredGraph(Graph wrappedGraph, Configuration config) {
        super(wrappedGraph, config, Config.INSTANCE, new HashMap<>() {{
            put(Methods.ADD_VERTEX, addVertexHandler(new BiFunction<Graph, Object[], Vertex>() {
                @Override
                public Vertex apply(final Graph graph, final Object[] keyValues) {
                    final BlockingQueue<Boolean> q = queues.get(MeterNames.IN_FLIGHT_OPERATIONS);
                    Boolean ticket = (Boolean) RuntimeUtil.errorHandled(q::take).get();
                    final Vertex x = graph.addVertex(keyValues);
                    try {
                        q.put(ticket);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return x;
                }
            }));
        }});
    }


    public static class Config extends ConfigurationBase {
        public static final MeteredGraph.Config INSTANCE = new MeteredGraph.Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(MeteredGraph.Config.Keys.class);
        }

        public static class Keys {
            public static final String GRAPH_PROVIDER_IMPL = WrappedGraph.Keys.GRAPH_PROVIDER_IMPL;
            public static final String BACKLOG_LIMIT = "output.graph.provider.backlog.limit";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(MeteredGraph.Config.Keys.BACKLOG_LIMIT, String.valueOf(1000)); //1 second delay
        }};
    }

    public static class MeterNames {
        public static final String IN_FLIGHT_OPERATIONS = "IN_FLIGHT_OPERATIONS";
    }

    public static final Map<String, AtomicLong> highWaterMark = new HashMap() {{
        put(MeterNames.IN_FLIGHT_OPERATIONS, new AtomicLong(0));

    }};
    public static Map<String, BlockingQueue<Boolean>> queues;

    public <T> LinkedBlockingQueue<T> fillQueue(final LinkedBlockingQueue<Object> queue, final int size, final Function<Integer, T> elementGetter) {
        IntStream.range(0, size).forEach(i -> RuntimeUtil.errorHandled(queue::add, () -> elementGetter.apply(i)));
        return (LinkedBlockingQueue<T>) queue;
    }

    public void init(Configuration config) {
        this.backlogLimit = Integer.parseInt(Config.INSTANCE.getOrDefault(Config.Keys.BACKLOG_LIMIT, config));
        queues = new HashMap() {{
            put(MeterNames.IN_FLIGHT_OPERATIONS, fillQueue(new LinkedBlockingQueue<>(backlogLimit), backlogLimit, (Function<Integer, Object>) integer -> true));
        }};
    }

    @Override
    public void barrier() {
        try {
            final int slotsRemaining = queues.get(MeterNames.IN_FLIGHT_OPERATIONS).size();
            highWaterMark.get(MeterNames.IN_FLIGHT_OPERATIONS).getAndUpdate(it -> it > backlogLimit - slotsRemaining ? it : backlogLimit - slotsRemaining);
            queues.get(MeterNames.IN_FLIGHT_OPERATIONS).take();
            queues.get(MeterNames.IN_FLIGHT_OPERATIONS).put(true);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeStatic() {
        highWaterMark.values().forEach(it -> it.set(0));
        queues.values().forEach(Collection::clear);
    }

    public void close() {
        closeStatic();
    }
}
