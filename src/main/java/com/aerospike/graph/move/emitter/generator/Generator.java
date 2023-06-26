package com.aerospike.graph.move.emitter.generator;

import com.aerospike.graph.move.emitter.*;
import com.aerospike.graph.move.emitter.generator.schema.SchemaParser;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.MovementIteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Generator extends Emitter.PhasedEmitter {

    // Configuration first.
    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String ROOT_VERTEX_ID_START = "emitter.rootVertexIdStart";
            public static final String ROOT_VERTEX_ID_END = "emitter.rootVertexIdEnd";
            public static final String ID_PROVIDER_END = "emitter.idProviderMax";
            public static final String SCHEMA_FILE = "emitter.schemaFile";
            public static final String ROOT_VERTEX_LABEL = "emitter.rootVertexLabel";
            public static final String CHANCE_TO_JOIN = "emitter.chanceToJoin";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.ROOT_VERTEX_ID_START, "0");
            put(Keys.ROOT_VERTEX_ID_END, "100");
            put(Keys.ID_PROVIDER_END, String.valueOf(Long.MAX_VALUE));
        }};
    }

    public static final Config CONFIG = new Config();
    private final Configuration config;

    //Static variables second.
    //...

    //Final class variables third

    private final Long rootVertexIdStart;
    private final Long rootVertexIdEnd;
    private final VertexSchema rootVertexSchema;
    private final GraphSchema graphSchema;

    //Mutable variables fourth.
    private Iterator<Object> idSupplier;

    //Constructor fifth.
    private Generator(final Configuration config) {
        this.config = config;
        this.rootVertexSchema = getRootVertexSchema(config);
        this.graphSchema = getGraphSchema(config);
        this.rootVertexIdStart = Long.valueOf(CONFIG.getOrDefault(config, Config.Keys.ROOT_VERTEX_ID_START));
        this.rootVertexIdEnd = Long.valueOf(CONFIG.getOrDefault(config, Config.Keys.ROOT_VERTEX_ID_END));
        this.idSupplier = MovementIteratorUtils.PrimitiveIteratorWrap.wrap(LongStream.range(rootVertexIdEnd + 1, Long.MAX_VALUE));
    }

    //Open, create or getInstance methods sixth.
    public static Generator open(final Configuration config) {
        return new Generator(config);
    }

    //Implementation seventh.
    @Override
    public Stream<Emitable> stream(Runtime.PHASE phase) {
        return stream(IteratorUtils.flatMap(getDriverForPhase(phase), List::iterator), phase);
    }

    @Override
    public Stream<Emitable> stream(Iterator<Object> iterator, Runtime.PHASE phase) {
        return IteratorUtils.stream(iterator)
                .flatMap(rootVertexId -> {
                    if (phase == Runtime.PHASE.ONE)
                        return Stream.of(new GeneratedVertex(true, (Long) rootVertexId, new VertexContext(graphSchema, rootVertexSchema, MovementIteratorUtils.wrapToLong(idSupplier))));
                    return Stream.empty();
                });
    }

    @Override
    public Iterator<List<Object>> getDriverForPhase(Runtime.PHASE phase) {
        if (phase.equals(Runtime.PHASE.ONE)) {
            if (phaseOneStarted.compareAndSet(false, true)) {
                return getOrCreateDriverIterator(phase,
                        (x) -> MovementIteratorUtils.wrap(LongStream.range(rootVertexIdStart, rootVertexIdEnd).iterator()));
            }
            return phaseOneIterator;
        } else if (phase.equals(Runtime.PHASE.TWO)) {
            if (phaseTwoStarted.compareAndSet(false, true)) {
                phaseTwoIterator = getOrCreateDriverIterator(phase,
                        (x) -> StitchProcess.idIterator(config));
            }
            return phaseTwoIterator;
        }
        throw new IllegalStateException("Unknown phase " + phase);
    }


    //Public methods eighth.

    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        return graphSchema.vertexTypes.stream()
                .filter(it -> Objects.equals(it.label, label))
                .findFirst().get()
                .properties.stream()
                .map(it -> it.name)
                .collect(Collectors.toList());
    }

    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return graphSchema.edgeTypes.stream()
                .filter(it -> Objects.equals(it.label, label))
                .findFirst().get()
                .properties.stream()
                .map(it -> it.name)
                .collect(Collectors.toList());
    }

    public static GraphSchema getGraphSchema(final Configuration config) {
        final String schemaFileLocation = CONFIG.getOrDefault(config, Config.Keys.SCHEMA_FILE);
        return SchemaParser.parse(Path.of(schemaFileLocation));
    }

    public static VertexSchema getRootVertexSchema(final Configuration config) {
        final GraphSchema schema = getGraphSchema(config);
        return schema.vertexTypes.stream()
                .filter(v -> v.label.equals(schema.entrypointVertexType))
                .findFirst().orElseThrow(() -> new RuntimeException("Could not find root vertex type"));
    }
    //Private methods ninth.
    //...

    //Inner classes tenth.
    //...

    //toString eleventh.
    @Override
    public String toString() {
        return this.getClass().getName();

    }

    //Close methods twelfth.
    @Override
    public void close() {

    }

}
