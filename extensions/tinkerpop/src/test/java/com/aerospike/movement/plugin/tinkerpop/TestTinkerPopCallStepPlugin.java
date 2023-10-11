package com.aerospike.movement.plugin.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.generator.Generator;
import com.aerospike.movement.emitter.generator.schema.YAMLParser;
import com.aerospike.movement.emitter.generator.schema.def.GraphSchema;
import com.aerospike.movement.emitter.generator.schema.def.VertexSchema;
import com.aerospike.movement.encoding.tinkerpop.GraphEncoder;
import com.aerospike.movement.output.tinkerpop.GraphOutput;
import com.aerospike.movement.plugin.Plugin;
import com.aerospike.movement.plugin.PluginInterface;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.process.tasks.generator.Generate;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.GeneratedOutputIdDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphTraversalProvider;
import com.aerospike.movement.util.core.*;
import com.aerospike.movement.util.core.iterator.ConfiguredRangeSupplier;
import com.aerospike.movement.util.core.iterator.OneShotSupplier;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.mock.task.MockTask;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import com.aerospike.movement.tinkerpop.common.PluginServiceFactory;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import com.aerospike.movement.util.generator.GeneratorUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.*;
import static com.aerospike.movement.test.mock.MockUtil.setDefaultMockCallbacks;
import static junit.framework.TestCase.*;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestTinkerPopCallStepPlugin extends AbstractMovementTest {

    final Map<String, String> mockConfigurationMap = getMockConfigurationMap();
    final AtomicBoolean failed = new AtomicBoolean(false);
    final List<Throwable> failures = new CopyOnWriteArrayList<>();

    @Before
    public void clearRuntimeSetFailure() {
        LocalParallelStreamRuntime.open(ConfigurationUtil.empty()).close();
        failures.clear();
        ErrorHandler.trigger.set(new Handler<Throwable>() {
            @Override
            public void handle(final Throwable e, final Object... context) {
                failures.add(e);
                failed.set(true);
                RuntimeUtil.halt();
            }
        });
    }

    @After
    public void cleanup() {
        failures.clear();
        failed.set(false);

    }


    @Test
    public void testLoadPluginLowLevel() {
        final Configuration testConfig = ConfigurationUtil.configurationWithOverrides(new MapConfiguration(mockConfigurationMap),
                new HashMap<>() {{
                    put(WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, String.valueOf(10+1));
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, String.valueOf(Long.MAX_VALUE));
                }});
        RuntimeUtil.lookupOrLoad(MockTask.class, testConfig);
        RuntimeUtil.getTaskClassByAlias(MockTask.class.getSimpleName());
        final Graph graph = SharedEmptyTinkerGraphGraphProvider.getInstance();
        final Plugin plugin = CallStepPlugin.open(testConfig);

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, 10).iterator())));
        graph.traversal().V().drop().iterate();

        graph.getServiceRegistry().registerService(PluginServiceFactory.create(
                (Task) RuntimeUtil.lookupOrLoad(MockTask.class, testConfig), plugin, graph, testConfig));

        graph.traversal()
                .call(MockTask.class.getSimpleName())
                .iterate();
    }

    @Test
    public void testLoadPlugin() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Configuration testConfig = ConfigurationUtil.configurationWithOverrides(new MapConfiguration(mockConfigurationMap),
                new HashMap<>() {{
                    put(WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, String.valueOf(10+1));
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, String.valueOf(Long.MAX_VALUE));
                }});
        RuntimeUtil.lookupOrLoad(MockTask.class, testConfig);
        RuntimeUtil.getTaskClassByAlias(MockTask.class.getSimpleName());
        final Graph graph = SharedEmptyTinkerGraphGraphProvider.getInstance();

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, 10).iterator())));
        graph.traversal().V().drop().iterate();

        final Object plugin = RuntimeUtil.openClassRef(CallStepPlugin.class.getName(), testConfig);

        plugin.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(plugin, graph);
        setDefaultMockCallbacks();
        graph.traversal()
                .call(MockTask.class.getSimpleName())
                .iterate();
    }


    @Test
    @Ignore
    public void testCallGeneratorPlugin() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().drop().iterate();
        SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal().V().drop().iterate();

        final Long ROOT_VERTEX_ID_MAX = 1000L;
        final Configuration testConfig = ConfigurationUtil.configurationWithOverrides(Generate.Config.INSTANCE.defaults(),
                new HashMap<>() {{
                    put(YAMLParser.Config.Keys.YAML_FILE_URI, IOUtil.copyFromResourcesIntoNewTempFile("example_schema.yaml").toURI().toString());
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, "1");
                    put(WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());
                    put(ConfigurationBase.Keys.ENCODER, GraphEncoder.class.getName());
                    put(GraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
                    put(OUTPUT, GraphOutput.class.getName());
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, String.valueOf(ROOT_VERTEX_ID_MAX+1));
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, String.valueOf(Long.MAX_VALUE));
                }});
        System.out.println(ConfigurationUtil.configurationToPropertiesFormat(testConfig));
        RuntimeUtil.lookupOrLoad(Generate.class, testConfig);
        RuntimeUtil.getTaskClassByAlias(Generate.class.getSimpleName());
        final Graph graph = SharedEmptyTinkerGraphGraphProvider.getInstance();

        SuppliedWorkChunkDriver.setIteratorSupplierForPhase(Runtime.PHASE.ONE, OneShotSupplier.of(() -> (Iterator<Object>) IteratorUtils.wrap(LongStream.range(0, ROOT_VERTEX_ID_MAX).iterator())));
        graph.traversal().V().drop().iterate();

        final Object plugin = RuntimeUtil.openClassRef(CallStepPlugin.class.getName(), testConfig);
        plugin.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(plugin, graph);

        graph.traversal()
                .call(Generate.class.getSimpleName())
                .iterate();

        assertEquals(ROOT_VERTEX_ID_MAX * 8, graph.traversal().V().count().next().longValue());
        final VertexSchema rootVertexSchema = Generator.getRootVertexSchema(testConfig);
        final List<String> vertexLabels = graph.traversal().V().label().dedup().toList();
        final List<String> edgeLabels = graph.traversal().E().label().dedup().toList();
        Generator.parseGraphSchema(testConfig).vertexTypes.stream().map(vt -> vt.label()).forEach(l -> {
            if (!vertexLabels.contains(l)) {
                throw new RuntimeException("Vertex label not found: " + l);
            }
        });
        Generator.parseGraphSchema(testConfig).edgeTypes.stream().map(et -> et.label()).forEach(l -> {
            if (!(edgeLabels.contains(l))) {
                throw new RuntimeException("Edge label not found: " + l);
            }
        });

        final Vertex aRootVertex = graph.traversal().V().hasLabel(rootVertexSchema.label()).limit(1).next();
        assertEquals(ROOT_VERTEX_ID_MAX, graph.traversal().V().hasLabel(rootVertexSchema.label()).count().next());
        System.out.println(aRootVertex);
        System.out.println("test");
    }



    @Test
    @Ignore
    public void testGenerateWithPlugin() throws MalformedURLException {

        SharedEmptyTinkerGraphGraphProvider.getInstance().traversal().V().drop().iterate();
        final File schemaFile = new File("../generator/src/main/resources/example_schema.yaml");
        final Long SCALE_FACTOR = 1000L;
        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);
                    put(PluginEnabledGraph.Config.Keys.GRAPH_IMPL, SharedEmptyTinkerGraphGraphProvider.class.getName());
                    put(OUTPUT, GraphOutput.class.getName());
                    put(WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
                    put(GraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
                    put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());
                    put(EMITTER, Generator.class.getName());
                    put(ENCODER, GraphEncoder.class.getName());
                    put(SuppliedWorkChunkDriver.Config.Keys.ITERATOR_SUPPLIER, ConfiguredRangeSupplier.class.getName());

                    put(SuppliedWorkChunkDriver.Config.Keys.RANGE_BOTTOM, 0L);
                    put(SuppliedWorkChunkDriver.Config.Keys.RANGE_TOP, SCALE_FACTOR);
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, SCALE_FACTOR * 10);
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, Long.MAX_VALUE);
                }});


        final Graph graph = PluginEnabledGraph.open(testConfig);

        graph.traversal().V().drop().iterate();
        graph.traversal()
                .call(Generate.class.getSimpleName())
                .with(Generate.Config.Keys.SCALE_FACTOR, SCALE_FACTOR)
                .with(YAMLParser.Config.Keys.YAML_FILE_URI, schemaFile.toURI())
                .iterate();
        final YAMLParser yamlParser = YAMLParser.open(ConfigurationUtil.configurationWithOverrides(testConfig, new HashMap<>() {{
            put(YAMLParser.Config.Keys.YAML_FILE_URI, schemaFile.toURI().toURL().toString());
        }}));
        GraphSchema schema = yamlParser.parse();

        final GeneratorUtil.GeneratedElementMetric vertexSchemaMetric = GeneratorUtil.vertexCountForScale(schema, SCALE_FACTOR);
        final GeneratorUtil.GeneratedElementMetric edgeSchemaMetric = GeneratorUtil.edgeCountForScale(schema, SCALE_FACTOR);

        if (failed.get()) {
            failures.forEach(it -> it.printStackTrace());
            fail("Failures recorded");
        }
//        TestCase.assertEquals((Long) 8000L, (Long) graph.traversal().V().count().next());
//        SchemaTestConstants
//                .verifyAllCounts((schemaLabel) -> {
//                    return graph.traversal().V().hasLabel(schemaLabel).hasNext() ?
//                            graph.traversal().V().hasLabel(schemaLabel).count().next() :
//                            graph.traversal().E().hasLabel(schemaLabel).count().next();
//                }, SCALE_FACTOR);
    }

}
