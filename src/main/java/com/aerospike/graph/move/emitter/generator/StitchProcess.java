package com.aerospike.graph.move.emitter.generator;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.output.OutputWriter;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.structure.EmittedIdImpl;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import com.aerospike.graph.move.util.GeneratorUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static com.aerospike.graph.move.util.GeneratorUtil.coinFlip;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class StitchProcess implements Emitter {
    final Map<String, Map<Long, Queue<Long>>> rememberedEdges = new ConcurrentHashMap<>();
    private final Configuration config;
    private final GraphSchema graphSchema;
    private final Output output;

    private StitchProcess(final Configuration config) {
        this.config = config;
        this.graphSchema = Generator.getGraphSchema(config);
        this.output = RuntimeUtil.loadOutput(config);
    }

    public StitchProcess open(final Configuration config) {
        return new StitchProcess(config);
    }

    public static Iterator<Object> idIterator(Configuration config) {
        throw ErrorUtil.unimplemented();
    }


    public Stream<EmittedIdImpl> outV(final GeneratedVertex rootVertex, final String stitchLabel) {
        return rememberedEdges
                .getOrDefault(stitchLabel, new ConcurrentHashMap<>())
                .getOrDefault(rootVertex.id, new ConcurrentLinkedQueue<>())
                .stream().map(EmittedIdImpl::new);
    }

    @Override
    public Stream<Emitable> stream(Runtime.PHASE phase) {
        return stream(IteratorUtils.stream(getDriverForPhase(phase)).flatMap(Collection::stream).iterator(), phase);
    }


    @Override
    public Stream<Emitable> stream(final Iterator<Object> iterator, final Runtime.PHASE phase) {
        if (!phase.equals(Runtime.PHASE.TWO))
            throw new IllegalStateException("StitchProcess only supports phase two");
        return IteratorUtils.stream(IteratorUtils.map(iterator,
                        o -> (Map.Entry<EmittedVertex, EmittedVertex>) o))
                .map(it -> encounter(it.getKey(), it.getValue()))
                .filter(Optional::isPresent).map(Optional::get);
    }

    @Override
    public Iterator<List<Object>> getDriverForPhase(Runtime.PHASE phase) {
        throw ErrorUtil.unimplemented();
    }

    private double getLikelyhoodToJoin(String labelA, String labelB) {
        //@todo should look at schema
        return Double.parseDouble(Generator.CONFIG.getOrDefault(config, Generator.Config.Keys.CHANCE_TO_JOIN));
    }

    private Optional<EmittedEdge> encounter(EmittedVertex a, EmittedVertex b) {
        if (coinFlip(getLikelyhoodToJoin(a.label(), b.label()))) {
            final EdgeSchema stitchSchema = GeneratorUtil.getStitchSchema(graphSchema, a.label(), b.label());
            final EdgeGenerator generator = new EdgeGenerator(stitchSchema, graphSchema);
            return Optional.of(generator.emit((OutputWriter) output, (Long) a.id().getId(), (Long) b.id().getId()));
        }
        return Optional.empty();
    }


    @Override
    public void close() {

    }

    @Override
    public List<String> getAllPropertyKeysForVertexLabel(String label) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public List<String> getAllPropertyKeysForEdgeLabel(String label) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public List<Runtime.PHASE> phases() {
        return List.of(Runtime.PHASE.TWO);
    }

}
