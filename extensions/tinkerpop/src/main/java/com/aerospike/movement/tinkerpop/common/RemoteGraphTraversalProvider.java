/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopTraversalEncoder;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoteGraphTraversalProvider implements TraversalProvider {
    private final Configuration config;

    public RemoteGraphTraversalProvider(Configuration config) {
        this.config = config;
    }

    @Override
    public GraphTraversalSource getProvided(GraphProvider.GraphProviderContext ctx) {
        final String host = Config.INSTANCE.getOrDefault(Config.Keys.HOST, config);
        final String port = Config.INSTANCE.getOrDefault(Config.Keys.PORT, config);
        final String remoteTraversalSourceName = Config.INSTANCE.getOrDefault(Config.Keys.REMOTE_TRAVERSAL_SOURCE_NAME, config);
        final GraphTraversalSource g = AnonymousTraversalSource
                .traversal()
                .withRemote(DriverRemoteConnection.using(host, Integer.parseInt(port), remoteTraversalSourceName));
        try {
            //test connection
            g.V().limit(1).hasNext();
        } catch (final Exception cannotConnect) {
            throw RuntimeUtil.getErrorHandler(GraphTraversalSource.class).handleFatalError(cannotConnect, config);
        }
        synchronized (RemoteGraphTraversalProvider.class) {
            if (Boolean.parseBoolean(Config.INSTANCE.getOrDefault(TinkerPopTraversalEncoder.Config.Keys.CLEAR, config))) {
                g.V().drop().iterate();
            }
        }
        return g;
    }


    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String HOST = "traversalSource.host";
            public static final String PORT = "traversalSource.port";
            public static final String REMOTE_TRAVERSAL_SOURCE_NAME = "traversalSource.remote.name";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(TinkerPopTraversalEncoder.Config.Keys.CLEAR, "true");
            put(Keys.REMOTE_TRAVERSAL_SOURCE_NAME, "g");
        }};
    }


    public static RemoteGraphTraversalProvider open(final Configuration config) {
        return new RemoteGraphTraversalProvider(config);
    }
}
