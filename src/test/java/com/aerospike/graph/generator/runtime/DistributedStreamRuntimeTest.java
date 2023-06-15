package com.aerospike.graph.generator.runtime;

import com.aerospike.graph.generator.AbstractGeneratorTest;
import com.aerospike.graph.generator.TestUtil;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.encoder.format.csv.CSVEncoder;
import com.aerospike.graph.generator.output.file.DirectoryOutput;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.IOUtil;
import org.apache.commons.configuration2.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

public class DistributedStreamRuntimeTest extends AbstractGeneratorTest {

    Configuration testCSVConfiguration;

    @Before
    public void setup() {
        testCSVConfiguration = ConfigurationBase.getCSVSampleConfiguration(testGraphSchemaLocationRelativeToModule(), TestUtil.createTempDirectory().toString());
//        testCSVConfiguration.setProperty(DistributedStreamRuntime.Config.Keys.MEMBERS_LIST, "localhost");
    }

    @Test
    public void createDistributedStreamRuntime() throws Exception {
        final DistributedStreamRuntime runtime = DistributedStreamRuntime.open(testCSVConfiguration);
        runtime.start().get();
        runtime.processVertexStream();
        runtime.close();
    }

    @Test
    public void createDistributedStreamRuntimeGroup() {

    }
}
