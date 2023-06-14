package com.aerospike.graph.generator;

import com.aerospike.graph.generator.common.tinkerpop.TinkerPopGraphProvider;
import com.aerospike.graph.generator.emitter.Emitter;
import com.aerospike.graph.generator.emitter.generated.Generator;
import com.aerospike.graph.generator.emitter.generated.StitchMemory;
import com.aerospike.graph.generator.emitter.tinkerpop.SourceGraph;
import com.aerospike.graph.generator.emitters.ClassicGraph;
import com.aerospike.graph.generator.encoder.format.csv.CSVEncoder;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.output.file.DirectoryOutput;
import com.aerospike.graph.generator.runtime.LocalParallelStreamRuntime;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Test;

import java.util.HashMap;
import java.util.Optional;

public class RoundTripTest extends AbstractGeneratorTest {

    private void mockRun(Configuration config) {
        final StitchMemory stitchMemory = new StitchMemory("none");
//        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(stitchMemory, 6, config);
        final Output output = RuntimeUtil.loadOutput(config);
        final Emitter emitter = RuntimeUtil.loadEmitter(config);
        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(config);

        runtime.processVertexStream();
        runtime.processEdgeStream();
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
            put(DirectoryOutput.Config.Keys.ENCODER, CSVEncoder.class.getName());
            put(ConfigurationBase.Keys.OUTPUT, DirectoryOutput.class.getName());
            put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, 100);
        }});
        mockRun(graphToCSVConfig);
    }

    @Test
    public void readFromGeneratorWriteToCsvLoadToGraph() {

    }

}
