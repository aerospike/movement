/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.generator;

import com.aerospike.movement.cli.CLI;
import com.aerospike.movement.encoding.files.csv.GraphCSVEncoder;
import com.aerospike.movement.output.files.DirectoryOutput;
import com.aerospike.movement.runtime.core.driver.impl.GeneratedOutputIdDriver;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.IOUtil;
import com.aerospike.movement.util.files.FileUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.*;
import static com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/*
  Created by Grant Haywood grant@iowntheinter.net
  7/23/23
*/
public class L2Integration extends AbstractMovementTest {
    @Test
    @Ignore //@todo remove
    public void testL2Integration() throws Exception {
        final String yamlFilePath = IOUtil.copyFromResourcesIntoNewTempFile("example_schema.yaml").getAbsolutePath();
        FileUtil.recursiveDelete(Path.of("/tmp/generate"));

        final String generateDir = "/tmp/generate";
        final Long scale = 160_000L;
        Configuration testConfig = new MapConfiguration(new HashMap<>() {{
            put(LocalParallelStreamRuntime.Config.Keys.THREADS, 16);
//            put(EMITTER, Generator.class.getName());
            put(WORK_CHUNK_DRIVER_PHASE_ONE, SuppliedWorkChunkDriver.class.getName());
//            put(YAMLSchemaParser.Config.Keys.YAML_FILE_PATH, yamlFilePath);
            put(BATCH_SIZE, 100);
//            put(Generator.Config.Keys.SCALE_FACTOR, scale);
            put(ENCODER, GraphCSVEncoder.class.getName());
            put(OUTPUT, DirectoryOutput.class.getName());
            put(OUTPUT_ID_DRIVER, GeneratedOutputIdDriver.class.getName());
            put(GeneratedOutputIdDriver.Config.Keys.RANGE_BOTTOM, String.valueOf(scale + 1));
            put(GeneratedOutputIdDriver.Config.Keys.RANGE_TOP, String.valueOf(Long.MAX_VALUE));
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, generateDir);
        }});

        final File tmpConfig = Files.createTempFile("testconfig", "properties").toFile();
        final String tmpConfigData = ConfigUtil.configurationToPropertiesFormat(testConfig);
        FileWriter fileWriter = new FileWriter(tmpConfig);
        fileWriter.write(tmpConfigData);
        fileWriter.close();
        final String VPATH = generateDir + "/vertices";
        final String EPATH = generateDir + "/edges";
        Path.of(generateDir).toFile().mkdirs();
        Path.of(VPATH).toFile().mkdir();
        Path.of(EPATH).toFile().mkdir();


        CLI.main(new String[]{"task=Generate", "-c", tmpConfig.getAbsolutePath()});

        assertTrue(Files.exists(Path.of(VPATH)));
        assertTrue(Files.exists(Path.of(EPATH)));

        final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using("172.17.0.1", 8182, "g"));
        g.V().drop().iterate();
        g.with("evaluationTimeout", 24 * 60 * 60 * 1000).call("bulk-load")
                .with("aerospike.graphloader.vertices", VPATH)
                .with("aerospike.graphloader.edges", EPATH)
                .iterate();

        Assert.assertEquals(8 * scale, g.V().count().next().longValue());
        Assert.assertEquals(7 * scale, g.E().count().next().longValue());
    }
}
