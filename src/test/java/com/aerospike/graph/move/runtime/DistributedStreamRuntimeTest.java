package com.aerospike.graph.move.runtime;

import com.aerospike.graph.move.AbstractGeneratorTest;
import com.aerospike.graph.move.TestUtil;
import com.aerospike.graph.move.runtime.distributed.DistributedStreamRuntime;
import com.aerospike.graph.move.config.ConfigurationBase;
import org.apache.commons.configuration2.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class DistributedStreamRuntimeTest extends AbstractGeneratorTest {

    Configuration testCSVConfiguration;

    @Before
    public void setup() {
        testCSVConfiguration = ConfigurationBase.getCSVSampleConfiguration(testGraphSchemaLocationRelativeToModule(), TestUtil.createTempDirectory().toString());
//        testCSVConfiguration.setProperty(DistributedStreamRuntime.Config.Keys.MEMBERS_LIST, "localhost");
    }

    @Test
    @Ignore
    public void createDistributedStreamRuntime() throws Exception {
        final DistributedStreamRuntime runtime = DistributedStreamRuntime.open(testCSVConfiguration);
        runtime.start().get();
        runtime.initialPhase().get();
        runtime.close();
    }

    @Test
    public void createDistributedStreamRuntimeGroup() {

    }
}
