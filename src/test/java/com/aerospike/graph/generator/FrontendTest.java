package com.aerospike.graph.generator;

import com.aerospike.graph.generator.output.file.DirectoryOutput;
import com.aerospike.graph.generator.util.IOUtil;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

public class FrontendTest extends AbstractGeneratorTest {
    Configuration testConfiguration;

    @Before
    public void setup() {
        testConfiguration = RuntimeUtil.loadConfiguration("../" + testGeneratorPropertiesLocationRelativeToProject());
        TestUtil.recursiveDelete(Path.of(testConfiguration.getString(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY)));
    }

    @Test
    @Ignore
    public void invokeTestCSV() throws InterruptedException, IOException {
        String[] args = {"-c", "../" + testGeneratorPropertiesLocationRelativeToProject()};
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
}
