/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.plugin.tinkerpop;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.files.DirectoryEmitter;
import com.aerospike.movement.emitter.files.RecursiveDirectoryTraversalDriver;
import com.aerospike.movement.encoding.files.csv.GraphCSVDecoder;
import com.aerospike.movement.encoding.tinkerpop.TinkerPopGraphEncoder;
import com.aerospike.movement.output.tinkerpop.TinkerPopGraphOutput;
import com.aerospike.movement.plugin.Plugin;
import com.aerospike.movement.plugin.PluginInterface;
import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.process.tasks.tinkerpop.Export;
import com.aerospike.movement.process.tasks.tinkerpop.Load;
import com.aerospike.movement.process.tasks.tinkerpop.Migrate;
import com.aerospike.movement.runtime.core.Handler;
import com.aerospike.movement.runtime.core.driver.impl.RangedOutputIdDriver;
import com.aerospike.movement.runtime.core.driver.impl.RangedWorkChunkDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.task.MockTask;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import com.aerospike.movement.test.tinkerpop.SharedTinkerClassicGraphProvider;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.runtime.IOUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.files.FileUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.movement.process.tasks.tinkerpop.Load.RECURSIVE_DIR_TRAVERSAL_CLASS_NAME;
import static org.junit.Assert.assertEquals;

public class TestTinkerPopCallStepPlugin extends AbstractMovementTest {

    final Map<String, ?> mockConfigurationMap = AbstractMovementTest.getMockConfigurationMap();
    final AtomicBoolean failed = new AtomicBoolean(false);
    final List<Throwable> failures = new CopyOnWriteArrayList<>();


    @Before
    public void clearRuntimeSetFailure() {
        LocalParallelStreamRuntime.closeStatic();
        failures.clear();
        ErrorHandler.trigger.set(new Handler<Throwable>() {
            @Override
            public void handle(final Throwable e, final Object... context) {
                failures.add(e);
                failed.set(true);
                RuntimeUtil.halt();
            }
        });
        LocalParallelStreamRuntime.getInstance(ConfigUtil.empty()).close();
    }

    @After
    public void cleanup() {
        failures.clear();
        failed.set(false);

    }


    @Test
    public void testClassNameCorrect() {
        assertEquals(RecursiveDirectoryTraversalDriver.class.getName(), RECURSIVE_DIR_TRAVERSAL_CLASS_NAME);
    }


    @Test
    public void testLoadPluginLowLevel() {
        final Configuration testConfig = ConfigUtil.withOverrides(new MapConfiguration(mockConfigurationMap),
                new HashMap<>() {{
                    put(ConfigurationBase.Keys.WORK_CHUNK_DRIVER_PHASE_ONE, RangedWorkChunkDriver.class.getName());
                    put(ConfigurationBase.Keys.OUTPUT_ID_DRIVER, RangedOutputIdDriver.class.getName());
                    put(RangedOutputIdDriver.Config.Keys.RANGE_BOTTOM, String.valueOf(10 + 1));
                    put(RangedOutputIdDriver.Config.Keys.RANGE_TOP, String.valueOf(Long.MAX_VALUE));
                    put(RangedWorkChunkDriver.Config.Keys.RANGE_BOTTOM, String.valueOf(0));
                    put(RangedWorkChunkDriver.Config.Keys.RANGE_TOP, String.valueOf(10));
                }});
        MockUtil.setDefaultMockCallbacks();
        RuntimeUtil.lookupOrLoad(MockTask.class, testConfig);
        RuntimeUtil.getTaskClassByAlias(MockTask.class.getSimpleName());
        final Graph graph = SharedEmptyTinkerGraphGraphProvider.open().getProvided(GraphProvider.GraphProviderContext.INPUT);
        final Plugin plugin = CallStepPlugin.open(testConfig);

        graph.traversal().V().drop().iterate();

        graph.getServiceRegistry().registerService(PluginServiceFactory.create(
                (Task) RuntimeUtil.lookupOrLoad(MockTask.class, testConfig), plugin, graph, testConfig));

        graph.traversal()
                .call(MockTask.class.getSimpleName())
                .iterate();
    }

    @Test
    public void testLoadPlugin() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
        Path tempPath = IOUtil.createTempDir();
        TinkerGraph classic = TinkerFactory.createClassic();
        final long classicVertexCount = classic.traversal().V().count().next();
        final long classicEdgeCount = classic.traversal().E().count().next();


        FileTestUtil.writeClassicGraphToDirectory(tempPath);

        final Map<String, String> testConfig =
                Load.Config.INSTANCE.defaultConfigMap(new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, String.valueOf(1)); //TinkerGraph is not thread safe
                    put(ConfigurationBase.Keys.EMITTER, DirectoryEmitter.class.getName());
                    put(ConfigurationBase.Keys.ENCODER, TinkerPopGraphEncoder.class.getName());
                    put(ConfigurationBase.Keys.DECODER, GraphCSVDecoder.class.getName());
                    put(ConfigurationBase.Keys.OUTPUT, TinkerPopGraphOutput.class.getName());
                    put(DirectoryEmitter.Config.Keys.BASE_PATH, tempPath.toString());
                    put(DirectoryEmitter.Config.Keys.PHASE_ONE_SUBDIR, "vertices");
                    put(DirectoryEmitter.Config.Keys.PHASE_TWO_SUBDIR, "edges");
                    put(TinkerPopGraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraphGraphProvider.class.getName());
                }});

        final Graph graph = SharedEmptyTinkerGraphGraphProvider.open().getProvided(GraphProvider.GraphProviderContext.INPUT);
        graph.traversal().V().drop().iterate();

        final Object plugin = RuntimeUtil.openClassRef(CallStepPlugin.class.getName(), new MapConfiguration(testConfig));

        plugin.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(plugin, graph);
        MockUtil.setDefaultMockCallbacks();
        Map<String, Object> x = (Map<String, Object>) graph.traversal()
                .call(Load.class.getSimpleName())
                .next();
        UUID id = (UUID) x.get("id");
        RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(x);
        List<Object> list = graph.traversal().call("--list").toList();
        Iterator<?> status = graph.traversal()
                .call(PluginServiceFactory.TASK_STATUS)
                .with(LocalParallelStreamRuntime.TASK_ID_KEY, id.toString());

        if (status.hasNext()) RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(status.next());
        RuntimeUtil.waitTask(id);

        System.out.printf("generated path: %s\n", tempPath.toAbsolutePath());
        Files.walk(tempPath).forEach(it -> {
            RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(it.toAbsolutePath());
            try {
                if (it.toFile().isFile())
                    Files.lines(it).forEach(System.out::println);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        long loadedVertexCount = graph.traversal().V().count().next();
        long loadedEdgeCount = graph.traversal().E().count().next();
        System.out.printf("%d vertices loaded, %d in source graph\n", loadedVertexCount, classicVertexCount);
        assertEquals(classicVertexCount, loadedVertexCount);
        System.out.printf("%d edges loaded, %d in source graph\n", loadedEdgeCount, classicEdgeCount);
        assertEquals(classicEdgeCount, loadedEdgeCount);
    }

    @Test
    public void testMigratePlugin() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {

        final Map<String, String> configMap =
                Migrate.Config.INSTANCE.defaultConfigMap(new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, String.valueOf(1)); //TinkerGraph is not thread safe
                }});

        final Graph graph = SharedEmptyTinkerGraphGraphProvider.open().getProvided(GraphProvider.GraphProviderContext.INPUT);
        graph.traversal().V().drop().iterate();
        final Configuration config = new MapConfiguration(configMap);
        final Object plugin = RuntimeUtil.openClassRef(CallStepPlugin.class.getName(), config);

        plugin.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(plugin, graph);
        RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(graph.traversal().call("--list").toList());

        MockUtil.setDefaultMockCallbacks();
        final GraphProvider inputGraphProvider = SharedTinkerClassicGraphProvider.open(config);
        long start = System.nanoTime();
        Map<String,Object> x = (Map<String, Object>) graph.traversal()
                .call(Migrate.class.getSimpleName())
                .with("emitter.tinkerpop.graph.provider", inputGraphProvider.getClass().getName())
                .with("encoder.graphProvider", SharedEmptyTinkerGraphGraphProvider.class.getName())
                .next();

        UUID id = (UUID) x.get("id");
        RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(x);

        Iterator<?> status = graph.traversal()
                .call(PluginServiceFactory.TASK_STATUS)
                .with(LocalParallelStreamRuntime.TASK_ID_KEY, id.toString());

        if (status.hasNext()) RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(status.next());
        RuntimeUtil.waitTask(id);
        long elapsed = System.nanoTime() - start;
        System.out.printf("elapsed time: %d ms\n", TimeUnit.NANOSECONDS.toMillis(elapsed));
        final long classicVertexCount = inputGraphProvider.getProvided(GraphProvider.GraphProviderContext.INPUT).traversal().V().count().next();
        final long classicEdgeCount = inputGraphProvider.getProvided(GraphProvider.GraphProviderContext.INPUT).traversal().E().count().next();

        long loadedVertexCount = graph.traversal().V().count().next();
        long loadedEdgeCount = graph.traversal().E().count().next();
        System.out.printf("%d vertices migrated, %d in source graph\n", loadedVertexCount, classicVertexCount);
        assertEquals(classicVertexCount, loadedVertexCount);
        System.out.printf("%d edges migrated, %d in source graph\n", loadedEdgeCount, classicEdgeCount);
        assertEquals(classicEdgeCount, loadedEdgeCount);
    }

    @Test
    public void testExportPlugin() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {

        final Map<String, String> configMap =
                Export.Config.INSTANCE.defaultConfigMap(new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, String.valueOf(8));
                }});

        final Graph graph = SharedEmptyTinkerGraphGraphProvider.open().getProvided(GraphProvider.GraphProviderContext.INPUT);
        graph.traversal().V().drop().iterate();
        final Configuration config = new MapConfiguration(configMap);
        final Object plugin = RuntimeUtil.openClassRef(CallStepPlugin.class.getName(), config);

        plugin.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(plugin, graph);
        RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(graph.traversal().call("--list").toList());

        MockUtil.setDefaultMockCallbacks();
        final GraphProvider inputGraphProvider = SharedTinkerClassicGraphProvider.open(config);
        final Path exportDir = Files.createTempDirectory("export");
        long start = System.nanoTime();
        Iterator<?> task = graph.traversal()
                .call(Export.class.getSimpleName())
                .with("output.directory", exportDir.toAbsolutePath().toString())
                .with("emitter.tinkerpop.graph.provider", inputGraphProvider.getClass().getName());
        Map<String,Object> map = (Map<String, Object>) task.next();
        UUID id = (UUID) map.get("id");
        RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(task.next());

        Iterator<?> status = graph.traversal()
                .call(PluginServiceFactory.TASK_STATUS)
                .with(LocalParallelStreamRuntime.TASK_ID_KEY, id.toString());

        if (status.hasNext()) RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(status.next());
        RuntimeUtil.waitTask(id);
        long elapsed = System.nanoTime() - start;
        System.out.printf("elapsed time: %d ms\n", TimeUnit.NANOSECONDS.toMillis(elapsed));
        final long classicVertexCount = inputGraphProvider.getProvided(GraphProvider.GraphProviderContext.INPUT).traversal().V().count().next();
        final long classicEdgeCount = inputGraphProvider.getProvided(GraphProvider.GraphProviderContext.INPUT).traversal().E().count().next();

        Files.walk(exportDir).filter(it -> it.toFile().isFile()).forEach(it -> RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(it.toAbsolutePath()));
        long filesWritten = Files.walk(exportDir).filter(it -> it.toFile().isFile()).count();
        long linesWritten = Files.walk(exportDir).filter(it -> it.toFile().isFile()).flatMap(it -> {
            try {
                return Files.lines(it);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).count();
        assertEquals(classicEdgeCount + classicVertexCount, linesWritten - filesWritten);
    }

}
