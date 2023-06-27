package com.aerospike.graph.move.format.csv;

import com.aerospike.graph.move.AbstractMovementTest;
import com.aerospike.graph.move.common.tinkerpop.ClassicGraph;
import com.aerospike.graph.move.common.tinkerpop.SharedEmptyTinkerGraph;
import com.aerospike.graph.move.common.tinkerpop.instrumentation.TinkerPopGraphProvider;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.fileLoader.DirectoryLoader;
import com.aerospike.graph.move.emitter.tinkerpop.SourceGraph;
import com.aerospike.graph.move.encoding.format.csv.GraphCSVDecoder;
import com.aerospike.graph.move.encoding.format.csv.GraphCSVEncoder;
import com.aerospike.graph.move.encoding.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.move.output.file.DirectoryOutput;
import com.aerospike.graph.move.output.tinkerpop.GraphOutput;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Set;

import static com.aerospike.graph.move.util.IOUtil.recursiveDelete;
import static junit.framework.TestCase.assertEquals;

public class CSVDecoderTest extends AbstractMovementTest {

    private Configuration getClassicGraphToCSVWriterConfiguration() {
        return new MapConfiguration(new HashMap<>() {{
            put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, "/tmp/generate");
            put(ConfigurationBase.Keys.EMITTER, SourceGraph.class.getName());
            put(SourceGraph.Config.Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
            put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, ClassicGraph.class.getName());
            put(DirectoryOutput.Config.Keys.ENCODER, GraphCSVEncoder.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, DirectoryOutput.class.getName());
            put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, 100);
        }});
    }

    private Configuration getCSVLoaderToGraphConfiguration() {
        return new MapConfiguration(new HashMap<>() {{
            put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);

            put(ConfigurationBase.Keys.EMITTER, DirectoryLoader.class.getName());
            put(DirectoryLoader.Config.Keys.VERTEX_FILE_PATH, DirectoryOutput.CONFIG.getDefaults().get(DirectoryOutput.Config.Keys.VERTEX_OUTPUT_DIRECTORY));
            put(DirectoryLoader.Config.Keys.EDGE_FILE_PATH, DirectoryOutput.CONFIG.getDefaults().get(DirectoryOutput.Config.Keys.EDGE_OUTPUT_DIRECTORY));
            put(ConfigurationBase.Keys.DECODER, GraphCSVDecoder.class.getName());

            put(ConfigurationBase.Keys.ENCODER, GraphEncoder.class.getName());
            put(GraphEncoder.Config.Keys.GRAPH_PROVIDER, SharedEmptyTinkerGraph.class.getName());
            put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, SharedEmptyTinkerGraph.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, GraphOutput.class.getName());
        }});
    }

    @Test
    public void testWriteReadCSVData() {
        recursiveDelete(Path.of("/tmp/generate"));
        final LocalParallelStreamRuntime writeCsvTask = new LocalParallelStreamRuntime(getClassicGraphToCSVWriterConfiguration());

        writeCsvTask.initialPhase().get();
        writeCsvTask.close();
        writeCsvTask.completionPhase().get();
        writeCsvTask.close();

        final LocalParallelStreamRuntime loadCSVTask = new LocalParallelStreamRuntime(getCSVLoaderToGraphConfiguration());

        loadCSVTask.initialPhase().get();
        loadCSVTask.close();
        loadCSVTask.completionPhase().get();
        loadCSVTask.close();

        final Graph loadedGraph = SharedEmptyTinkerGraph.getInstance();

        final TinkerGraph classicGraph = TinkerFactory.createClassic();

        assertEquals(classicGraph.traversal().V().count().next(), loadedGraph.traversal().V().count().next());
        assertEquals(classicGraph.traversal().E().count().next(), loadedGraph.traversal().E().count().next());
        classicGraph.vertices().forEachRemaining(v -> {
            Long longId = ((Integer)v.id()).longValue();
            assertEquals(v.label(), loadedGraph.vertices(longId).next().label());
            Set<String> a = v.keys();
            Set<String> b = loadedGraph.vertices(longId).next().keys();
            assertEquals(a.size(), b.size());
            v.keys().forEach(k -> {
                assertEquals(v.value(k).toString(), loadedGraph.vertices(longId).next().value(k).toString());
            });
            assertEquals(IteratorUtils.count(v.edges(Direction.IN)), IteratorUtils.count(loadedGraph.vertices(longId).next().edges(Direction.IN)));
            assertEquals(IteratorUtils.count(v.edges(Direction.OUT)), IteratorUtils.count(loadedGraph.vertices(longId).next().edges(Direction.OUT)));
            assertEquals(IteratorUtils.count(v.properties()), IteratorUtils.count(loadedGraph.vertices(longId).next().properties()));
        });
    }
}
