package com.aerospike.graph.generator.emitters;

import com.aerospike.graph.generator.emitter.tinkerpop.SourceGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClassicGraphProvider implements SourceGraph.GraphProvider {
    private Map<String, List<String>> edgeLabelCache = new ConcurrentHashMap<>();
    private Map<String, List<String>> vertexLabelCache = new ConcurrentHashMap<>();

    static Graph crewGraph = TinkerFactory.createClassic();
    private final Configuration config;

    public ClassicGraphProvider(Configuration config) {
        this.config = config;
    }

    @Override
    public Graph getGraph() {
        return crewGraph;
    }

    @Override
    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        return vertexLabelCache.computeIfAbsent(label, k ->
                crewGraph.traversal().V()
                        .hasLabel(label)
                        .properties()
                        .key()
                        .dedup()
                        .toList());
    }

    @Override
    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return edgeLabelCache.computeIfAbsent(label, k ->
                crewGraph.traversal().E()
                        .hasLabel(label)
                        .properties()
                        .key()
                        .dedup()
                        .toList());
    }


    public static SourceGraph.GraphProvider open(Configuration config) {
        return new ClassicGraphProvider(config);
    }
}
