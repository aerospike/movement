package com.aerospike.graph.move.process;

import com.aerospike.graph.move.AbstractMovementTest;
import com.aerospike.graph.move.CLI;
import com.aerospike.graph.move.common.tinkerpop.SharedEmptyTinkerGraph;
import com.aerospike.graph.move.common.tinkerpop.instrumentation.TinkerPopGraphProvider;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.encoding.format.csv.GraphCSVEncoder;
import com.aerospike.graph.move.encoding.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.move.output.file.DirectoryOutput;
import com.aerospike.graph.move.output.tinkerpop.GraphOutput;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static com.aerospike.graph.move.CLI.TEST_MODE;

public class BatchJobTest extends AbstractMovementTest {
    Configuration configA = new MapConfiguration(new HashMap<>() {{
        put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 20L);
        put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
        put(Generator.Config.Keys.SCHEMA_FILE, newGraphSchemaLocationRelativeToModule());
        put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, "/tmp/generateA");
        put(DirectoryOutput.Config.Keys.ENCODER, GraphCSVEncoder.class.getName());
        put(ConfigurationBase.Keys.OUTPUT, DirectoryOutput.class.getName());
        put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, 100);
        put(CLI.TEST_MODE, true);
    }});
    Configuration configB = new MapConfiguration(new HashMap<>() {{
        put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 20L);
        put(ConfigurationBase.Keys.EMITTER, Generator.class.getName());
        put(Generator.Config.Keys.SCHEMA_FILE, newGraphSchemaLocationRelativeToModule());
        put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, "/tmp/generateB");
        put(DirectoryOutput.Config.Keys.ENCODER, GraphCSVEncoder.class.getName());
        put(ConfigurationBase.Keys.OUTPUT, DirectoryOutput.class.getName());
        put(DirectoryOutput.Config.Keys.ENTRIES_PER_FILE, 100);
        put(CLI.TEST_MODE, true);
    }});

    @Test
    public void createBatchJob() {
        BatchJob.of(configA, configB).run();
    }

    @Test
    public void batchByOverrides() {
        BatchJob.of(configA).withOverrides(new HashMap<>() {{
            put("generateA", new HashMap<>() {{
                put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 30L);
            }});
            put("generateB", new HashMap<>() {{
                put(Generator.Config.Keys.ROOT_VERTEX_ID_END, 40L);
            }});
        }}).run();

    }
}