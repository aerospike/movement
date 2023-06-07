package com.aerospike.graph.generator.util;

import com.aerospike.graph.generator.emitter.tinkerpop.SourceGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TinkerPopGraphProvider implements SourceGraph.GraphProvider {
    public static final Config CONFIG = new Config();
    private Map<String, List<String>> vertexLabelCache = new ConcurrentHashMap<>();
    private Map<String, List<String>> edgeLabelCache = new ConcurrentHashMap<>();


    public static class Config extends ConfigurationBase {

        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String GRAPH_IMPL = "graph.provider.impl";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }

    private final Configuration config;
    private final Graph graph;

    public TinkerPopGraphProvider(Configuration config) {
        this.config = config;
        this.graph = (Graph) RuntimeUtil.openClassRef(CONFIG.getOrDefault(config, Config.Keys.GRAPH_IMPL), config);
    }

    public static SourceGraph.GraphProvider open(Configuration config) {
        return new TinkerPopGraphProvider(config);
    }

    @Override
    public Graph getGraph() {
        return graph;
    }

    @Override
    public List<String> getAllPropertyKeysForVertexLabel(String label) {
        return vertexLabelCache.computeIfAbsent(label, k ->
                graph.traversal().V()
                        .hasLabel(label)
                        .properties()
                        .key()
                        .dedup()
                        .toList());
    }

    @Override
    public List<String> getAllPropertyKeysForEdgeLabel(String label) {
        return edgeLabelCache.computeIfAbsent(label, k ->
                graph.traversal().E()
                        .hasLabel(label)
                        .properties()
                        .key()
                        .dedup()
                        .toList());
    }
}
