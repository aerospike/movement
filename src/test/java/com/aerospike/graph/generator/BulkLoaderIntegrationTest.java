package com.aerospike.graph.generator;

import com.aerospike.graph.generator.common.tinkerpop.TinkerPopGraphProvider;
import com.aerospike.graph.generator.emitter.Emitter;
import com.aerospike.graph.generator.emitter.generated.Generator;
import com.aerospike.graph.generator.emitter.generated.StitchMemory;
import com.aerospike.graph.generator.emitter.generated.schema.Parser;
import com.aerospike.graph.generator.emitter.generated.schema.def.VertexSchema;
import com.aerospike.graph.generator.emitter.tinkerpop.SourceGraph;
import com.aerospike.graph.generator.emitters.ClassicGraph;
import com.aerospike.graph.generator.encoder.format.csv.CSVEncoder;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.output.file.DirectoryOutput;
import com.aerospike.graph.generator.runtime.LocalParallelStreamRuntime;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.IOUtil;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.assertEquals;

public class BulkLoaderIntegrationTest extends AbstractGeneratorTest {

    private static final String BULK_LOADER_MAIN_CLASS = "com.aerospike.firefly.bulkloader.SparkBulkLoader";
    static private final String DEFAULT_CONFIG_REL = "src/test/resources/config-generator.properties";
    private static final String[] DEFAULT_PARAMS = {"-dryrun", "-writeedge", "-writevertex", "-supernode", "-verifyedge", "-verifyvertex"};

    @Before
    public void delete() {
        IOUtil.recursiveDelete(Path.of("/tmp/generate"));
    }


    @Test
    @Ignore
    public void BulkLoaderIntegration() {
        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 20L);
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, "/tmp/generate");
            put(ConfigurationBase.Keys.EMITTER, SourceGraph.class.getName());
            put(SourceGraph.Config.Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
            put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, ClassicGraph.class.getName());
            put(DirectoryOutput.Config.Keys.ENCODER, CSVEncoder.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, DirectoryOutput.class.getName());
            put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, 100);
        }});
        final StitchMemory stitchMemory = new StitchMemory("none");
//        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(stitchMemory, 6, config);
        final Output output = RuntimeUtil.loadOutput(config);
        final Emitter emitter = RuntimeUtil.loadEmitter(config);
        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(config);
        runtime.processVertexStream();
        runtime.processEdgeStream();

        System.out.println(output);
        output.close();
        Configuration fireflyConfig = RuntimeUtil.loadConfiguration(DEFAULT_CONFIG_REL);
        final Graph fireflyGraph = (Graph) RuntimeUtil.openClassRef("com.aerospike.firefly.structure.FireflyGraph", fireflyConfig);
        fireflyGraph.traversal().V().drop().iterate();
        RuntimeUtil.invokeClassMain(BULK_LOADER_MAIN_CLASS, buildArgs(DEFAULT_CONFIG_REL, DEFAULT_PARAMS));
        final TinkerGraph classicGraph = TinkerFactory.createClassic();
        // The correct number of verticies have moved from the TinkerGraph to the CSV files
        assertEquals((Long) classicGraph.traversal().V().count().next(), (Long) output.getVertexMetric());
        // The correct number of edges have moved from the csv files to the FireflyGraph
        assertEquals((Long) fireflyGraph.traversal().E().count().next(), (Long) output.getEdgeMetric());
    }

    @Test
    @Ignore
    public void BulkLoaderIntegrationParallel() {
        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 20L);
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, "/tmp/generate");
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
            put(DirectoryOutput.Config.Keys.ENCODER, CSVEncoder.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, DirectoryOutput.class.getName());
            put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, 1000);
            put(Generator.Config.Keys.SCHEMA_FILE, newGraphSchemaLocationRelativeToModule());
        }});
        final StitchMemory stitchMemory = new StitchMemory("none");
        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(config);

        runtime.processVertexStream();
        runtime.processEdgeStream();

        final List<Output> outputs = List.copyOf(runtime.getOutputMap().values());
        outputs.forEach(Output::close);
        outputs.forEach(System.err::println);

        Configuration fireflyConfig = RuntimeUtil.loadConfiguration(DEFAULT_CONFIG_REL);
        final Graph fireflyGraph = (Graph) RuntimeUtil.openClassRef("com.aerospike.firefly.structure.FireflyGraph", fireflyConfig);
        fireflyGraph.traversal().V().drop().iterate();
        RuntimeUtil.invokeClassMain(BULK_LOADER_MAIN_CLASS, buildArgs(DEFAULT_CONFIG_REL, DEFAULT_PARAMS));
        final TinkerGraph classicGraph = TinkerFactory.createClassic();
        final Long writtenEdges = outputs.stream().map(Output::getEdgeMetric).reduce(0L, Long::sum);
        final Long writtenVerticies = outputs.stream().map(Output::getVertexMetric).reduce(0L, Long::sum);
        // The correct number of verticies have moved from the TinkerGraph to the CSV files
//        assertEquals((Long) classicGraph.traversal().V().count().next(), (Long) writtenVerticies);
        // The correct number of edges have moved from the csv files to the FireflyGraph
        assertEquals((Long) fireflyGraph.traversal().E().count().next(), (Long) writtenEdges);
        final GraphTraversalSource g = fireflyGraph.traversal();
        schema = Parser.parse(Path.of(newGraphSchemaLocationRelativeToModule()));
        final VertexSchema type = schema.vertexTypes.iterator().next();
        final String entryPointLabel = type.label;
        final Vertex entrypoint = g.V().hasLabel(entryPointLabel).next();
        assertEquals(Long.valueOf(type.outEdges.size()), (Long) g.V(entrypoint).outE().count().next());
        System.out.println(entrypoint);
    }

    @Test
    @Ignore
    public void BulkLoaderGeneratorIntegration() {
        Configuration config = new MapConfiguration(new HashMap<>() {{
            put(Generator.Config.Keys.SCHEMA_FILE, testGraphSchemaLocationRelativeToModule());
//            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 2000000L); // 1G
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 200000L); // 100M
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, "/tmp/generate");
            put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
            put(DirectoryOutput.Config.Keys.ENCODER, CSVEncoder.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, DirectoryOutput.class.getName());
            put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, 100000);
        }});
        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(config);
        final Output output = RuntimeUtil.loadOutput(config);
        final Emitter emitter = RuntimeUtil.loadEmitter(config);
//        final LocalSequentialStreamRuntime runtime = new LocalSequentialStreamRuntime(config, stitchMemory, Optional.of(output), Optional.of(emitter));
        IOUtil.recursiveDelete(Path.of("/tmp/generate"));
        runtime.processVertexStream();
        runtime.processEdgeStream();
        System.out.println(output);
        output.close();
        Configuration fireflyConfig = RuntimeUtil.loadConfiguration(DEFAULT_CONFIG_REL);
        final Graph fireflyGraph = (Graph) RuntimeUtil.openClassRef("com.aerospike.firefly.structure.FireflyGraph", fireflyConfig);
        fireflyGraph.traversal().V().drop().iterate();
        RuntimeUtil.invokeClassMain(BULK_LOADER_MAIN_CLASS, buildArgs(DEFAULT_CONFIG_REL, DEFAULT_PARAMS));
        // The correct number of verticies have moved from the TinkerGraph to the CSV files
//        assertEquals((Long) 600L, (Long) output.getVertexMetric());
        // The correct number of edges have moved from the csv files to the FireflyGraph
//        assertEquals((Long) fireflyGraph.traversal().E().count().next(), (Long) output.getEdgeMetric());
        final VertexSchema rootVertexSchema = schema.vertexTypes.stream()
                .filter(it ->
                        it.label.equals(schema.entrypointVertexType)).findFirst().get();
        final String labelFromSchema = rootVertexSchema.label;

//        final List<String> keysForLabel = fireflyGraph.traversal().V().hasLabel(labelFromSchema).properties().key().dedup().toList();
//        assertEquals((int) schema.vertexTypes.stream()
//                        .filter(it -> it.label.equals(labelFromSchema))
//                        .flatMap(it -> it.properties.stream())
//                        .map(it -> it.name).count(),
//                keysForLabel.size());
    }

    private Object[] buildArgs(String defaultConfig, String[] defaultParams) {
        String[] x = new String[]{"-local", "-c", defaultConfig};
        Object[] y = ArrayUtils.addAll(x, defaultParams);

        return y;
    }


}
