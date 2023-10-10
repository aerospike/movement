package com.aerospike.movement.emitter.generator;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.generator.schema.YAMLParser;
import com.aerospike.movement.encoding.tinkerpop.TraversalEncoder;
import com.aerospike.movement.output.tinkerpop.TraversalOutput;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.GeneratedOutputIdDriver;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.test.tinkerpop.SharedEmptyTinkerGraphTraversalProvider;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.IOUtil;
import com.aerospike.movement.util.core.iterator.ConfiguredRangeSupplier;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.*;
import static junit.framework.TestCase.assertEquals;

public class SchemaGraphIntegration {
    @Test
    public void testSimplest(){

    }

    @Test
    public void remoteGDemoSchema() throws IOException {
        final Long SCALE_FACTOR = 1L;
        final File schemaFile = IOUtil.copyFromResourcesIntoNewTempFile("simplest_schema.yaml");

        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(YAMLParser.Config.Keys.YAML_FILE_PATH, schemaFile.getAbsolutePath());
                    put(LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE, 1);
                    put(EMITTER, Generator.class.getName());
                    put(ConfigurationBase.Keys.ENCODER, TraversalEncoder.class.getName());
                    put(TraversalEncoder.Config.Keys.TRAVERSAL_PROVIDER, SharedEmptyTinkerGraphTraversalProvider.class.getName());
                    put(ConfigurationBase.Keys.OUTPUT, TraversalOutput.class.getName());

                    put(WORK_CHUNK_DRIVER, SuppliedWorkChunkDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());
                    put(SuppliedWorkChunkDriver.Config.Keys.ITERATOR_SUPPLIER, ConfiguredRangeSupplier.class.getName());

                    put(SuppliedWorkChunkDriver.Config.Keys.RANGE_BOTTOM, 0L);
                    put(SuppliedWorkChunkDriver.Config.Keys.RANGE_TOP, SCALE_FACTOR);
                    put(ConfiguredRangeSupplier.Config.Keys.RANGE_BOTTOM, 0L);
                    put(ConfiguredRangeSupplier.Config.Keys.RANGE_TOP, SCALE_FACTOR);
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, SCALE_FACTOR * 10);
                    put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, Long.MAX_VALUE);
                }});
        System.out.println(ConfigurationUtil.configurationToPropertiesFormat(testConfig));

        final GraphTraversalSource g = SharedEmptyTinkerGraphTraversalProvider.getGraphInstance().traversal();
        g.V().drop().iterate();


        final Runtime runtime = LocalParallelStreamRuntime.open(testConfig);
        final Iterator<RunningPhase> x = runtime.runPhases(List.of(Runtime.PHASE.ONE), testConfig);
        while (x.hasNext()) {
            final RunningPhase y = x.next();
            IteratorUtils.iterate(y);
            y.get();
            y.close();
        }
        runtime.close();
        assertEquals(2L, g.V().count().next().longValue());
        assertEquals(1L, g.E().count().next().longValue());
    }
}
