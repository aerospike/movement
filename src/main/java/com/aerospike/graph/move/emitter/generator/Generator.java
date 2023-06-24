package com.aerospike.graph.move.emitter.generator;

import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.emitter.generator.schema.SchemaParser;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.util.ConfigurationBase;
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
//@todo root vertices can have in edges

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Generator implements Emitter {
    private final Long rootVertexIdStart;
    private final Long rootVertexIdEnd;
    public static final Config CONFIG = new Config();

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


    private final VertexSchema rootVertexSchema;
    private final GraphSchema graphSchema;
    private Iterator<Long> idSupplier;

    private Generator( final Configuration config) {
        this.rootVertexSchema = getRootVertexSchema(config);
        this.graphSchema = getGraphSchema(config);
        this.rootVertexIdStart = Long.valueOf(CONFIG.getOrDefault(config, Config.Keys.ROOT_VERTEX_ID_START));
        this.rootVertexIdEnd = Long.valueOf(CONFIG.getOrDefault(config, Config.Keys.ROOT_VERTEX_ID_END));
        this.idSupplier = LongStream.range(rootVertexIdEnd + 1, Long.MAX_VALUE).iterator();
    }

    public VertexSchema getRootVertexSchema() {
        return rootVertexSchema;
    }

    public static Generator open(Configuration config) {
        return new Generator( config);
    }

    @Override
    public Stream<EmittedVertex> phaseOneStream() {
        return LongStream
                .range(rootVertexIdStart, rootVertexIdEnd)
                .mapToObj(rootVertexId ->
                        (EmittedVertex) new GeneratedVertex(true, rootVertexId,
                                new VertexContext(graphSchema, rootVertexSchema, idSupplier)));
    }

    @Override
    public Stream<EmittedVertex> phaseOneStream(final long startId, final long endId) {
        return LongStream
                .range(startId, endId)
                .mapToObj(rootVertexId ->
                        (EmittedVertex) new GeneratedVertex(true, rootVertexId,
                                new VertexContext(graphSchema, rootVertexSchema, idSupplier)));
    }

    @Override
    public Stream<EmittedEdge> phaseTwoStream() {
        return Stream.empty();
    }

    @Override
    public Emitter withIdSupplier(Iterator<List<?>> idSupplier) {
        return this.idSupplier = idSupplier;
    }


    @Override
    public void close() {

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
    public static GraphSchema getGraphSchema(Configuration config) {
        final String schemaFileLocation = CONFIG.getOrDefault(config, Config.Keys.SCHEMA_FILE);
        return SchemaParser.parse(Path.of(schemaFileLocation));
    }
    public static VertexSchema getRootVertexSchema(Configuration config) {
        final GraphSchema schema = getGraphSchema(config);
        return schema.vertexTypes.stream()
                .filter(v -> v.label.equals(schema.entrypointVertexType))
                .findFirst().orElseThrow(() -> new RuntimeException("Could not find root vertex type"));    }

}
