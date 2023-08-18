package com.aerospike.movement.tinkerpop.common;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.encoding.tinkerpop.TraversalEncoder;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoteGraphTraversalProvider {
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
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String HOST = "traversalSource.host";
            public static final String PORT = "traversalSource.port";
            public static final String REMOTE_TRAVERSAL_SOURCE_NAME = "traversalSource.remote.name";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(TraversalEncoder.Config.Keys.CLEAR, "true");
            put(Keys.REMOTE_TRAVERSAL_SOURCE_NAME, "g");
        }};
    }

    public static GraphTraversalSource open(final Configuration config) {
        final String host = Config.INSTANCE.getOrDefault(Config.Keys.HOST, config);
        final String port = Config.INSTANCE.getOrDefault(Config.Keys.PORT, config);
        final String remoteTraversalSourceName = Config.INSTANCE.getOrDefault(Config.Keys.REMOTE_TRAVERSAL_SOURCE_NAME, config);
        final GraphTraversalSource g = AnonymousTraversalSource
                .traversal()
                .withRemote(DriverRemoteConnection
                        .using(host, Integer.parseInt(port), remoteTraversalSourceName));
        synchronized (RemoteGraphTraversalProvider.class) {
            if (Boolean.parseBoolean(Config.INSTANCE.getOrDefault(TraversalEncoder.Config.Keys.CLEAR, config))) {
                g.V().drop().iterate();
            }
        }
        return g;
    }
}
