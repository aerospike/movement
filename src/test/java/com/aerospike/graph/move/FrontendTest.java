package com.aerospike.graph.move;

import com.aerospike.graph.move.output.file.DirectoryOutput;
import com.aerospike.graph.move.util.IOUtil;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.IntStream;

public class FrontendTest extends AbstractGeneratorTest {
    Configuration testConfiguration;

    @Before
    public void setup() {
        testConfiguration = RuntimeUtil.loadConfiguration(testGeneratorPropertiesLocationRelativeToProject());
        IOUtil.recursiveDelete(Path.of(testConfiguration.getString(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY)));
    }

    @Test
    @Ignore
    public void invokeTestCSV() throws InterruptedException, IOException {
        String[] args = {"-c", testGeneratorPropertiesLocationRelativeToProject()};
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
        String[] args = {"-c", testGeneratorTraversalPropertiesLocationRelativeToProject()};
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
