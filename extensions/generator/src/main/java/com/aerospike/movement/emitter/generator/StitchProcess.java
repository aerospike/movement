package com.aerospike.movement.emitter.generator;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.emitter.core.graph.EmittedEdge;
import com.aerospike.movement.emitter.core.graph.EmittedVertex;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.structure.core.EmittedIdImpl;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ErrorUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.emitter.generator.schema.def.EdgeSchema;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.util.generator.GeneratorUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static com.aerospike.movement.util.generator.GeneratorUtil.coinFlip;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

/**
 * @// TODO: 7/8/23
 * StichProcess should use DirectoyLoader as the underlying emitter if its writing to a directory
 * There needs to be a way to turn an Output into an Emitter to do the second pass
 */
public class StitchProcess extends Loadable implements Emitter {
    final Map<String, Map<Long, Queue<Long>>> rememberedEdges = new ConcurrentHashMap<>();
    private final Configuration config;
    private final GraphSchema graphSchema;
    private final Output output;

    private StitchProcess(final Configuration config) {
        super(MockOutput.Config.INSTANCE, config);
        this.config = config;
        this.graphSchema = Generator.parseGraphSchema(config);
        this.output = RuntimeUtil.loadOutput(config);
    }

    public static StitchProcess open(final Configuration config) {
        return new StitchProcess(config);
    }


    public Stream<EmittedIdImpl> outV(final GeneratedVertex rootVertex, final String stitchLabel) {
        return rememberedEdges
                .getOrDefault(stitchLabel, new ConcurrentHashMap<>())
                .getOrDefault(rootVertex.id, new ConcurrentLinkedQueue<>())
                .stream().map(EmittedIdImpl::new);
    }


    @Override
    public Stream<Emitable> stream(final WorkChunkDriver workChunkDriver, final Runtime.PHASE phase) {
//        if (!phase.equals(Runtime.PHASE.TWO))
//            throw new IllegalStateException("StitchProcess only supports phase two");
//        return IteratorUtils.stream(IteratorUtils.map(driver,
//                        o -> (Map.Entry<EmittedVertex, EmittedVertex>) o))
//                .map(it -> encounter(it.getKey(), it.getValue()))
//                .filter(Optional::isPresent).map(Optional::get);
        throw ErrorUtil.unimplemented();
    }

    private double getLIKELIHOODToJoin(String labelA, String labelB) {
        //@todo should look at schema
        return Double.parseDouble(Generator.Config.INSTANCE.getOrDefault(Generator.Config.Keys.CHANCE_TO_JOIN, config));
    }

    private Optional<EmittedEdge> encounter(EmittedVertex a, EmittedVertex b) {
        if (coinFlip(getLIKELIHOODToJoin(a.label(), b.label()))) {
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

    @Override
    public void init(final Configuration config) {

    }
}
