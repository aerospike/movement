package com.aerospike.movement.emitter.files;

import com.aerospike.movement.emitter.tinkerpop.TinkerPopGraphEmitter;
import com.aerospike.movement.encoding.files.csv.GraphCSVEncoder;
import com.aerospike.movement.output.files.DirectoryOutput;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.impl.RangedOutputIdDriver;
import com.aerospike.movement.runtime.core.local.LocalParallelStreamRuntime;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import com.aerospike.movement.runtime.tinkerpop.TinkerPopGraphDriver;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.tinkerpop.SharedTinkerClassicGraphProvider;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.aerospike.movement.config.core.ConfigurationBase.Keys.*;


public class FileTestUtil {
    public static final String TINKERPOP_GRAPH_EMITTER_CLASS = "com.aerospike.movement.emitter.tinkerpop.TinkerPopGraphEmitter";
    public static final String CLASSIC_GRAPH_PROVIDER_CLASS = "com.aerospike.movement.test.tinkerpop.ClassicGraphProvider";
    public static final String TINKERPOP_GRAPH_DRIVER_CLASS = "com.aerospike.movement.runtime.tinkerpop.TinkerPopGraphDriver";
//    public static final String GRAPH_PROVIDER = "emitter.graphProvider";


    public static void writeClassicGraphToDirectory(Path outputDirectory) {

        final Configuration testConfig = new MapConfiguration(
                new HashMap<>() {{
                    put(LocalParallelStreamRuntime.Config.Keys.THREADS, 1);
                    put(LocalParallelStreamRuntime.Config.Keys.BATCH_SIZE, 1);
                    put(EMITTER, TinkerPopGraphEmitter.class.getName());
                    put(TinkerPopGraphEmitter.Config.Keys.GRAPH_PROVIDER, SharedTinkerClassicGraphProvider.class.getName());
                    put(ENCODER, GraphCSVEncoder.class.getName());
                    put(OUTPUT, DirectoryOutput.class.getName());

                    put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, outputDirectory.toString());
                    put(WORK_CHUNK_DRIVER_PHASE_ONE, TinkerPopGraphDriver.class.getName());
                    put(WORK_CHUNK_DRIVER_PHASE_TWO, TinkerPopGraphDriver.class.getName());
                    put(OUTPUT_ID_DRIVER, RangedOutputIdDriver.class.getName());
                }});
        System.out.println(ConfigUtil.configurationToPropertiesFormat(testConfig));


        final Runtime runtime = LocalParallelStreamRuntime.open(testConfig);
        final Iterator<RunningPhase> x = runtime.runPhases(List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO), testConfig);
        AbstractMovementTest.iteratePhasesAndCloseRuntime(x, runtime);
        try {
            System.out.println(Files.list(outputDirectory).collect(Collectors.toList()));
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        System.out.println("eot");
    }
}
