package com.aerospike.graph.move.emitter.generator;

import com.aerospike.graph.move.emitter.*;
import com.aerospike.graph.move.emitter.generator.schema.SchemaParser;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.ErrorUtil;
import com.aerospike.graph.move.util.MovementIteratorUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration2.Configuration;

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
public class Generator implements Emitter {
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
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.ROOT_VERTEX_ID_START, "0");
            put(Keys.ROOT_VERTEX_ID_END, "100");
            put(Keys.ID_PROVIDER_END, String.valueOf(Long.MAX_VALUE));
        }};
    }

    public static final Config CONFIG = new Config();

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
    public Stream<Emitable> phaseOneStream() {
        return phaseOneStream(rootVertexIdStart, rootVertexIdEnd);
    }

    @Override
    public Stream<Emitable> phaseOneStream(final long startId, final long endId) {
        return LongStream
                .range(startId, endId)
                .mapToObj(rootVertexId ->
                        new GeneratedVertex(true, rootVertexId,
                                new VertexContext(graphSchema, rootVertexSchema, MovementIteratorUtils.wrapToLong(idSupplier))));
    }

    @Override
    public Stream<Emitable> phaseTwoStream() {
        //@todo join subgraphs
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Stream<Emitable> phaseTwoStream(long startId, long endId) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Iterator<Object> phaseOneIterator() {
        return idSupplier;
    }

    @Override
    public Iterator<Object> phaseTwoIterator() {
        return IteratorUtils.emptyIterator();
    }

    @Override
    public Emitter withIdSupplier(Iterator<Object> idSupplier) {
        this.idSupplier = idSupplier;
        return this;
    }

    @Override
    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        return graphSchema.vertexTypes.stream()
                .filter(it -> Objects.equals(it.label, label))
                .findFirst().get()
                .properties.stream()
                .map(it -> it.name)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return graphSchema.edgeTypes.stream()
                .filter(it -> Objects.equals(it.label, label))
                .findFirst().get()
                .properties.stream()
                .map(it -> it.name)
                .collect(Collectors.toList());
    }

    //Public methods eighth.
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
