package com.aerospike.graph.generator.runtime;

import com.aerospike.graph.generator.AbstractGeneratorTest;
import com.aerospike.graph.generator.TestUtil;
import com.aerospike.graph.generator.emitter.generated.StitchMemory;
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
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ParallelStreamRuntimeTest extends AbstractGeneratorTest {
    Configuration testCSVConfiguration;

    @Before
    public void setup() {
        testCSVConfiguration = ConfigurationBase.getCSVSampleConfiguration(testGraphSchemaLocationRelativeToModule(), TestUtil.createTempDirectory().toString());
    }

    @Test
    public void writeToCSVTest() throws IOException {
        final long startTime = System.currentTimeMillis();

        final StitchMemory stitchMemory = new StitchMemory("none");
        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(stitchMemory, testCSVConfiguration);
        final Encoder<String> encoder = CSVEncoder.open(testCSVConfiguration);
        final Iterator<CapturedError> x = runtime.processVertexStream().iterator();
        while (x.hasNext())
            System.out.println(x.next());
        final long stopTime = System.currentTimeMillis();
        runtime.close();
        final Path directory = Path.of(testCSVConfiguration.getString(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY));
        final long vertexEntries = Files.walk(directory.resolve("vertices")).filter(Files::isRegularFile).flatMap(file -> {
            try {
                return Files.lines(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).count();
        final long edgeEntries = Files.walk(directory.resolve("edges")).filter(Files::isRegularFile).flatMap(file -> {
            try {
                return Files.lines(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).count();

//        assertEquals(6040, vertexEntries); //@todo why is this off by 10 from sequential test?
//        assertEquals(5040, edgeEntries);

        Files.walk(directory.resolve("vertices")).filter(Files::isRegularFile).forEach(it -> {
            String label = it.getFileName().toString().split("_")[0];
            try {
                String metadata = encoder.encodeVertexMetadata(label);
                if(!Files.readAllLines(it).stream().filter(line -> line.equals(metadata)).iterator().hasNext())
                    throw new Exception("Metadata not found in file: " + it);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        final long totalTime = stopTime - startTime;
        final long writtenSize = IOUtil.calculateDirectorySize(directory.toAbsolutePath().toString());
        System.out.println(runtime);
        System.out.println("Total time: " + totalTime / 1000 + "s");
        System.out.println("Total size: " + writtenSize / 1024 / 1024 + " MB");
        System.out.println("MB/s: " + (writtenSize / totalTime) / 1000);
    }
}
