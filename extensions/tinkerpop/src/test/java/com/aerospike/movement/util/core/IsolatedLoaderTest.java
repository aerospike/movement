/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.util.core;

import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

public class IsolatedLoaderTest {
    @Test
    public void testIsolatedClassLoading() throws ClassNotFoundException, MalformedURLException {
        URLClassLoader tinkerGraphJarLoader = new URLClassLoader(new URL[]{new File("src/test/resources/tinkergraph-gremlin-3.6.6.jar").toURL()}, Thread.currentThread().getContextClassLoader());
        Class<?> c1 = tinkerGraphJarLoader.loadClass(TinkerGraph.class.getName());
        Graph graph = (Graph) RuntimeUtil.openClass(c1, new MapConfiguration(Map.of()));
        RuntimeUtil.getLogger().info(graph.variables());
    }
}
