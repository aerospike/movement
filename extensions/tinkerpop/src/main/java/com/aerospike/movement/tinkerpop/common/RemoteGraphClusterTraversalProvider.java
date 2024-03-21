/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopTraversalEncoder;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class RemoteGraphClusterTraversalProvider implements TraversalProvider {
    private final Configuration config;

    public RemoteGraphClusterTraversalProvider(Configuration config) {
        this.config = config;
    }

    public static ConcurrentHashMap<String, GraphTraversalSource> shared = new ConcurrentHashMap<>();
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

        public Object uri() {
            return uri;
        }

        public String toString() {
            return uri.toString();
        }
    }

    @Override
    public GraphTraversalSource getProvided(GraphProvider.GraphProviderContext ctx) {
        final String URI_LIST;
        if (ctx.equals(GraphProvider.GraphProviderContext.INPUT)) {

            URI_LIST = Config.Keys.INPUT_URI_LIST;
        }else{

            URI_LIST = Config.Keys.OUTPUT_URI_LIST;
        }

        Cluster.Builder builder = Cluster.build();
        String traversalSourceName = "";
        for (String uristr:  config.getString(URI_LIST).split(";")){
            URI uri = URI.create(uristr);
            RemoteGraphTraversalProvider.URIConnectionInfo uriInfo = RemoteGraphTraversalProvider.URIConnectionInfo.from(uri);
            if(traversalSourceName.isEmpty())
                traversalSourceName = uriInfo.traversalSourceName;
            else if(!traversalSourceName.equals(uriInfo.traversalSourceName))
                throw new RuntimeException("Cluster endpoints must have uniform traversal source name");
            builder.addContactPoint(uriInfo.host).port(uriInfo.port);
        }

        final Cluster cluster = builder.create();
        final DriverRemoteConnection drc = DriverRemoteConnection.using(cluster, traversalSourceName);
//        final GraphTraversalSource g = shared.computeIfAbsent(URI_LIST,it -> traversal().withRemote(drc));
        final GraphTraversalSource g = traversal().withRemote(drc);

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
            return ConfigUtil.getKeysFromClass(Keys.class);
        }


        public static class Keys {
            public static final String INPUT_URI_LIST = "movement.provider.graph.traversal.remote.uri.input.list";

            public static final String OUTPUT_URI_LIST = "movement.provider.graph.traversal.remote.uri.output.list";

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(TinkerPopTraversalEncoder.Config.Keys.CLEAR, "false");
        }};
    }


    public static RemoteGraphClusterTraversalProvider open(final Configuration config) {
        return new RemoteGraphClusterTraversalProvider(config);
    }
}
