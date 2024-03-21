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

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoteGraphTraversalProvider implements TraversalProvider {
    private final Configuration config;

    public RemoteGraphTraversalProvider(Configuration config) {
        this.config = config;
    }

    public static class URIConnectionInfo {
        public final String host;
        public final int port;
        public final String traversalSourceName;
        private final URI uri;

        public URIConnectionInfo(URI uri) {
            this.uri = uri;
            this.host = uri.getHost();
            this.port = uri.getPort();
            this.traversalSourceName = uri.getPath().split("/")[1];
        }

        public static URIConnectionInfo from(URI uri) {
            return new URIConnectionInfo(uri);
        }
        public static URIConnectionInfo from(String host, int port, String traversalSourceName, boolean ssl) {
            String scheme = ssl?"wss":"ws";
            URI uri = URI.create(String.format("%s://%s:%d/%s",scheme,host,port,traversalSourceName));
            return new URIConnectionInfo(uri);
        }

        public URI uri() {
            return uri;
        }

        public String toString() {
            return uri.toString();
        }
    }

    @Override
    public GraphTraversalSource getProvided(GraphProvider.GraphProviderContext ctx) {
        final String CTX_URI, CTX_HOST, CTX_PORT, CTX_NAME;
        if (ctx.equals(GraphProvider.GraphProviderContext.INPUT)) {
            CTX_PORT = Config.Keys.INPUT_PORT;
            CTX_HOST = Config.Keys.INPUT_HOST;
            CTX_NAME = Config.Keys.INPUT_REMOTE_TRAVERSAL_SOURCE_NAME;
            CTX_URI = Config.Keys.INPUT_URI;
        }else{
            CTX_PORT = Config.Keys.OUTPUT_PORT;
            CTX_HOST = Config.Keys.OUTPUT_HOST;
            CTX_NAME = Config.Keys.OUTPUT_REMOTE_TRAVERSAL_SOURCE_NAME;
            CTX_URI = Config.Keys.OUTPUT_URI;
        }

        if (config.containsKey(CTX_URI)) {
            final URI uri = URI.create(config.getString(CTX_URI));
            URIConnectionInfo uriInfo = URIConnectionInfo.from(uri);
            config.setProperty(CTX_HOST, uriInfo.host);
            config.setProperty(CTX_PORT, uriInfo.port);
            config.setProperty(CTX_NAME, uriInfo.traversalSourceName);
        }
        final String host = Config.INSTANCE.getOrDefault(CTX_HOST, config);
        final String port = Config.INSTANCE.getOrDefault(CTX_PORT, config);
        final String remoteTraversalSourceName = Config.INSTANCE.getOrDefault(CTX_NAME, config);
        final GraphTraversalSource g = AnonymousTraversalSource
                .traversal()
                .withRemote(DriverRemoteConnection.using(host, Integer.parseInt(port), remoteTraversalSourceName));
        try {
            //test connection
            g.V().limit(1).hasNext();
        } catch (final Exception cannotConnect) {
            throw RuntimeUtil.getErrorHandler(GraphTraversalSource.class).handleFatalError(cannotConnect, config);
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
            public static final String INPUT_URI = "movement.provider.graph.traversal.remote.uri.input";
            public static final String INPUT_HOST = "traversalSource.host.input";
            public static final String INPUT_PORT = "traversalSource.port.input";
            public static final String INPUT_REMOTE_TRAVERSAL_SOURCE_NAME = "traversalSource.remote.name.input";
            public static final String OUTPUT_URI = "movement.provider.graph.traversal.remote.uri.output";
            public static final String OUTPUT_HOST = "traversalSource.host.output";
            public static final String OUTPUT_PORT = "traversalSource.port.output";
            public static final String OUTPUT_REMOTE_TRAVERSAL_SOURCE_NAME = "traversalSource.remote.name.output";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(TinkerPopTraversalEncoder.Config.Keys.CLEAR, "false");
            put(Keys.INPUT_REMOTE_TRAVERSAL_SOURCE_NAME, "g");
            put(Keys.OUTPUT_REMOTE_TRAVERSAL_SOURCE_NAME, "g");
        }};
    }


    public static RemoteGraphTraversalProvider open(final Configuration config) {
        return new RemoteGraphTraversalProvider(config);
    }
}
