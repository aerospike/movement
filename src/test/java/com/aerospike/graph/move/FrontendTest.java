package com.aerospike.graph.move;

import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.generator.Generator;
import com.aerospike.graph.move.output.file.DirectoryOutput;
import com.aerospike.graph.move.util.IOUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

public class FrontendTest extends AbstractGeneratorTest {
    Configuration testConfiguration;

    @Before
    public void setup() throws IOException {
        testConfiguration = ConfigurationBase.getCSVSampleConfiguration(newGraphSchemaLocationRelativeToModule(), Files.createTempDirectory("test").toAbsolutePath().toString());
        IOUtil.recursiveDelete(Path.of(testConfiguration.getString(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY)));
    }

    @Test
    public void invokeTestCSV() throws IOException {
        String[] args = {
                "-c", sampleConfigurationLocationRelativeToModule(),
                "-o", String.format("%s=%s", DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, testConfiguration.getString(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY)),
                "-o", String.format("%s=%s", Generator.Config.Keys.ROOT_VERTEX_ID_END, 40000),
                "-o", String.format("%s=%s", Generator.Config.Keys.SCHEMA_FILE, newGraphSchemaLocationRelativeToModule())
        };
        final long startTime = System.currentTimeMillis();
        CLI.main(args);
        final long stopTime = System.currentTimeMillis();
        final long totalTime = stopTime - startTime;
        final Path directory = Path.of(testConfiguration.getString(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY));
        final long writtenSize = IOUtil.calculateDirectorySize(directory.toAbsolutePath().toString());
        System.out.println("Total time: " + totalTime / 1000 + "s");
        System.out.println("Total size: " + writtenSize / 1024 / 1024 + " MB");
        System.out.println("MB/s: " + (writtenSize / totalTime) / 1000);
    }

    @Test
    @Ignore
    public void invokeTestRemoteGraph() {
        String[] args = {"-c", sampleConfigurationLocationRelativeToModule()};
        IntStream.range(0, 100).forEach(i -> {
            System.out.println("pass");
            final long startTime = System.currentTimeMillis();
            try {
                CLI.main(args);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            final long stopTime = System.currentTimeMillis();
            final long totalTime = stopTime - startTime;
            System.out.println("Total time: " + totalTime / 1000 + "s");
        });
    }

}
