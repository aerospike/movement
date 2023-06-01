package com.aerospike.graph.generator.emitter.generated;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.emitter.Emitter;
import com.aerospike.graph.generator.emitter.generated.schema.Parser;
import com.aerospike.graph.generator.emitter.generated.schema.def.GraphSchema;
import com.aerospike.graph.generator.emitter.generated.schema.def.VertexSchema;
import com.aerospike.graph.generator.util.ConfigurationBase;
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

    private Generator(final VertexSchema rootVertexSchema, final GraphSchema schema, final Configuration config) {
        this.rootVertexSchema = rootVertexSchema;
        this.graphSchema = schema;
        this.rootVertexIdStart = Long.valueOf(CONFIG.getOrDefault(config, Config.Keys.ROOT_VERTEX_ID_START));
        this.rootVertexIdEnd = Long.valueOf(CONFIG.getOrDefault(config, Config.Keys.ROOT_VERTEX_ID_END));
        this.idSupplier = LongStream.range(rootVertexIdEnd + 1, Long.MAX_VALUE).iterator();
    }

    public VertexSchema getRootVertexSchema() {
        return rootVertexSchema;
    }

    public static Generator open(Configuration config) {
        final String schemaFileLocation = CONFIG.getOrDefault(config, Config.Keys.SCHEMA_FILE);
        final GraphSchema schema = Parser.parse(Path.of(schemaFileLocation));

        final VertexSchema rootVertexSchema = schema.vertexTypes.stream()
                .filter(v -> v.label.equals(schema.entrypointVertexType))
                .findFirst().orElseThrow(() -> new RuntimeException("Could not find root vertex type"));
        return new Generator(rootVertexSchema, schema, config);
    }

    @Override
    public Stream<EmittedVertex> vertexStream() {
        return LongStream
                .range(rootVertexIdStart, rootVertexIdEnd)
                .mapToObj(rootVertexId ->
                        (EmittedVertex) new GeneratedVertex(true, rootVertexId,
                                new VertexContext(graphSchema, rootVertexSchema, idSupplier)));
    }

    @Override
    public Stream<EmittedVertex> vertexStream(final long startId, final long endId) {
        return LongStream
                .range(startId, endId)
                .mapToObj(rootVertexId ->
                        (EmittedVertex) new GeneratedVertex(true, rootVertexId,
                                new VertexContext(graphSchema, rootVertexSchema, idSupplier)));
    }

    @Override
    public Stream<EmittedEdge> edgeStream() {
        return Stream.empty();
    }

    @Override
    public Emitter withIdSupplier(final Iterator<Long> idSupplier) {
        this.idSupplier = idSupplier;
        return this;
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
}
