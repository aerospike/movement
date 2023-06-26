package com.aerospike.graph.move;

import com.aerospike.graph.move.common.tinkerpop.instrumentation.TinkerPopGraphProvider;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.emitter.generator.StitchMemory;
import com.aerospike.graph.move.emitter.tinkerpop.SourceGraph;
import com.aerospike.graph.move.emitters.ClassicGraph;
import com.aerospike.graph.move.encoding.format.csv.GraphCSV;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.output.file.DirectoryOutput;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Test;

import java.util.HashMap;

public class RoundTripTest extends AbstractGeneratorTest {

    private void mockRun(Configuration config) {
        final StitchMemory stitchMemory = new StitchMemory("none");
//        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(stitchMemory, 6, config);
        final Output output = RuntimeUtil.loadOutput(config);
        final Emitter emitter = RuntimeUtil.loadEmitter(config);
        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(config);

        runtime.initialPhase();
        runtime.completionPhase();
        System.out.println(output);
        output.close();
    }


    @Test
    public void readFromGraphWriteToCsvLoadBackInGraph() {
        Configuration graphToCSVConfig = new MapConfiguration(new HashMap<>() {{
            put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 1000L);
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, "/tmp/generate");
            put(ConfigurationBase.Keys.EMITTER, SourceGraph.class.getName());
            put(SourceGraph.Config.Keys.GRAPH_PROVIDER, TinkerPopGraphProvider.class.getName());
            put(TinkerPopGraphProvider.Config.Keys.GRAPH_IMPL, ClassicGraph.class.getName());
            put(DirectoryOutput.Config.Keys.ENCODER, GraphCSV.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, DirectoryOutput.class.getName());
            put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, 100);
        }});
        mockRun(graphToCSVConfig);
    }

    @Test
    public void readFromGeneratorWriteToCsvLoadToGraph() {

    }

}
