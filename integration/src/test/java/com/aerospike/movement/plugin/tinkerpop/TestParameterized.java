package com.aerospike.movement.plugin.tinkerpop;

import com.aerospike.movement.plugin.PluginInterface;
import com.aerospike.movement.process.tasks.tinkerpop.Export;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphGraphProvider;
import com.aerospike.movement.test.tinkerpop.SharedTinkerClassicGraphProvider;
import com.aerospike.movement.tinkerpop.common.GraphProvider;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestParameterized {

    final static Integer testLoops = 20;

    private final Integer threadCount;

    @Parameterized.Parameters
    public static Collection<Integer[]> data() {
        final List<Integer[]> testParams = new ArrayList<>();
        IntStream.range(0, testLoops).forEach(loop -> {
            testParams.add(new Integer[]{
                    (1 + new Random().nextInt(RuntimeUtil.getAvailableProcessors() * 2))
            });
        });
        return testParams;
    }

    public TestParameterized(final Integer threadCount) {

        this.threadCount = threadCount;
    }

    @Test
    public void testExportPlugin() throws Exception {

        final Map<String, String> configMap =
                Export.Config.INSTANCE.defaultConfigMap(new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, String.valueOf(threadCount));
                }});



        final Graph controller = TinkerGraph.open();

        final Configuration config = new MapConfiguration(configMap);
        final Object plugin = RuntimeUtil.openClassRef(CallStepPlugin.class.getName(), config);

        plugin.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(plugin, controller);
        RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(controller.traversal().call("--list").toList());

        MockUtil.setDefaultMockCallbacks();
        final Path exportDir = Files.createTempDirectory("export");
        long start = System.nanoTime();
        Iterator<?> task = controller.traversal()
                .call(Export.class.getSimpleName())
                .with("output.directory", exportDir.toAbsolutePath().toString())
                .with("emitter.tinkerpop.graph.provider", SharedTinkerClassicGraphProvider.class.getName());
        Map<String, Object> map = (Map<String, Object>) task.next();
        UUID id = (UUID) map.get("id");
        RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(task.next());

        Iterator<?> status = controller.traversal()
                .call(PluginServiceFactory.TASK_STATUS)
                .with(LocalParallelStreamRuntime.TASK_ID_KEY, id.toString());

        if (status.hasNext()) RuntimeUtil.getLogger(this.getClass().getSimpleName()).info(status.next());
        RuntimeUtil.waitTask(id);
        controller.close();

        LocalParallelStreamRuntime.closeStatic();
        long elapsed = System.nanoTime() - start;
        Graph classic = TinkerFactory.createClassic();
        System.out.printf("elapsed time: %d ms\n", TimeUnit.NANOSECONDS.toMillis(elapsed));

        final long classicVertexCount = classic.traversal().V().count().next();
        final long classicEdgeCount = classic.traversal().E().count().next();

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
