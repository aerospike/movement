package com.aerospike.graph.generator.emitter.generated;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class StitchMemory {
    private final String stitchType;

    final Map<String, Map<Long, Queue<Long>>> rememberedEdges = new ConcurrentHashMap<>();

    public StitchMemory(final String stitchType) {
        this.stitchType = stitchType;
    }





    public Stream<GeneratedVertex.GeneratedVertexId> outV(final GeneratedVertex rootVertex, final String stitchLabel) {
        return rememberedEdges
                .getOrDefault(stitchLabel, new ConcurrentHashMap<>())
                .getOrDefault(rootVertex.id, new ConcurrentLinkedQueue<>())
                .stream().map(GeneratedVertex.GeneratedVertexId::new);
    }
}
