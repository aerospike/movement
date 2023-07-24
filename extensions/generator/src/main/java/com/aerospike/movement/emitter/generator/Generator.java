package com.aerospike.movement.emitter.generator;


import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.emitter.generator.schema.Parser;
import com.aerospike.movement.emitter.generator.schema.YAMLParser;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.*;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.emitter.generator.schema.def.VertexSchema;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Generator extends Loadable implements Emitter {


    @Override
    public void init(final Configuration config) {

    }

    // Configuration first.
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
            public static final String SCALE_FACTOR = "generator.scaleFactor";
            public static final String CHANCE_TO_JOIN = "generator.chanceToJoin";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.SCALE_FACTOR, "100");
        }};
    }

    private final Configuration config;

    //Static variables
    //...

    //Final class variables
    private final OutputIdDriver outputIdDriver;

    private final Long scaleFactor;
    private final VertexSchema rootVertexSchema;
    private final GraphSchema graphSchema;

    //Mutable variables

    //Constructor
    private Generator(final OutputIdDriver outputIdDriver, final Configuration config) {
        super(MockOutput.Config.INSTANCE, config);
        this.config = config;
        this.rootVertexSchema = getRootVertexSchema(config);
        this.graphSchema = getGraphSchema(config);
        this.scaleFactor = Long.valueOf(Config.INSTANCE.getOrDefault(Config.Keys.SCALE_FACTOR, config));
        this.outputIdDriver = outputIdDriver;
    }

    //Open, create or getInstance methods
    public static Generator open(final Configuration config) {
        return create(config);
    }

    public static Generator create(final Configuration config) {
        return new Generator((OutputIdDriver) RuntimeUtil.lookupOrLoad(OutputIdDriver.class, config), config);
    }


    //Public static methods.
    public static Configuration getEmitterConfig() {
        return new MapConfiguration(new HashMap<>() {{
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
        }});
    }

    //Implementation seventh.

    @Override
    public Stream<Emitable> stream(final WorkChunkDriver workChunkDriver, final Runtime.PHASE phase) {
        if (phase == Runtime.PHASE.ONE) {
            final Iterator<WorkChunk> chunks = workChunkDriver.iterator();
            final Iterator<Long> rootIds = IteratorUtils.map(IteratorUtils.flatMap(chunks, chunk -> chunk.iterator()), workId -> (Long) workId.getId());
            final Iterator<Emitable> emitables = IteratorUtils.map(rootIds, rootId -> new GeneratedVertex(rootId, new VertexContext(graphSchema, rootVertexSchema, outputIdDriver)));
            return IteratorUtils.stream(emitables);
        } else {
            return Stream.empty();
        }
    }


    @Override
    public List<Runtime.PHASE> phases() {
        return List.of(Runtime.PHASE.ONE);
    }

    //Public methods eighth.


    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        return graphSchema.vertexTypes.stream()
                .filter(it -> Objects.equals(it.label(), label))
                .findFirst().orElseThrow(() -> new RuntimeException("Could not find vertex type " + label + graphSchema))
                .properties.stream()
                .map(it -> it.name)
                .collect(Collectors.toList());
    }

    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return graphSchema.edgeTypes.stream()
                .filter(it -> Objects.equals(it.label(), label))
                .findFirst().orElseThrow(() -> new RuntimeException("Could not find edge type" + label))
                .properties.stream()
                .map(it -> it.name)
                .collect(Collectors.toList());
    }


    public static GraphSchema getGraphSchema(final Configuration config) {
        final Parser yamlParser = (Parser) RuntimeUtil.openClassRef(YAMLParser.class.getName(), config);
        return yamlParser.parse();
    }

    public static VertexSchema getRootVertexSchema(final Configuration config) {
        final GraphSchema schema = getGraphSchema(config);
        return getRootVertexSchema(schema);
    }

    public static VertexSchema getRootVertexSchema(final GraphSchema schema) {
        return schema.vertexTypes.stream()
                .filter(v -> v.label().equals(schema.entrypointVertexType))
                .findFirst().orElseThrow(() -> new RuntimeException("Could not find root vertex type"));
    }
    //Private static methods.

    //Public methods

    //Private methods
    //...

    //Inner classes
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
