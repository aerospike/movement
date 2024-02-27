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
    Path exportDir;
    private static long totalTime = 0;

    @Parameterized.Parameters
    public static Collection<Integer[]> data() {
        final List<Integer[]> testParams = new ArrayList<>();
        IntStream.range(0, testLoops).forEach(loop -> {
            testParams.add(new Integer[]{
                    (1)//+ new Random().nextInt(RuntimeUtil.getAvailableProcessors() * 2))
            });
        });
        return testParams;
    }

    public TestParameterized( final Integer threadCount) {

        this.threadCount = threadCount;
    }

    @Test
    public void testExportPlugin() throws Exception {

        final Map<String, String> configMap =
                Export.Config.INSTANCE.defaultConfigMap(new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, String.valueOf(threadCount));
                }});

        final Graph graph = SharedEmptyTinkerGraphGraphProvider.getGraphInstance();
        graph.traversal().V().drop().iterate();
        final Configuration config = new MapConfiguration(configMap);
        final Object plugin = RuntimeUtil.openClassRef(CallStepPlugin.class.getName(), config);

        plugin.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(plugin, graph);
        System.out.println(graph.traversal().call("--list").toList());

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
        System.out.println(task.next());

        Iterator<?> status = graph.traversal()
                .call(PluginServiceFactory.TASK_STATUS)
                .with(LocalParallelStreamRuntime.TASK_ID_KEY, id.toString());

        if (status.hasNext()) System.out.println(status.next());
        RuntimeUtil.waitTask(id);
        graph.close();
        SharedEmptyTinkerGraphGraphProvider.getGraphInstance().close();
        LocalParallelStreamRuntime.closeStatic();
        long elapsed = System.nanoTime() - start;
        System.out.printf("elapsed time: %d ms\n", TimeUnit.NANOSECONDS.toMillis(elapsed));
        final long classicVertexCount = inputGraphProvider.getProvided(GraphProvider.GraphProviderContext.INPUT).traversal().V().count().next();
        final long classicEdgeCount = inputGraphProvider.getProvided(GraphProvider.GraphProviderContext.INPUT).traversal().E().count().next();

        Files.walk(exportDir).filter(it -> it.toFile().isFile()).forEach(it -> System.out.println(it.toAbsolutePath()));
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
