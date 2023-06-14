package com.aerospike.graph.generator.runtime;

import com.aerospike.graph.generator.AbstractGeneratorTest;
import com.aerospike.graph.generator.TestUtil;
import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.encoder.format.csv.CSVEncoder;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.output.OutputWriter;
import com.aerospike.graph.generator.output.file.DirectoryOutput;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.IOUtil;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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

        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(testCSVConfiguration);
        final Encoder<String> encoder = CSVEncoder.open(testCSVConfiguration);
        runtime.processVertexStream();
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
                if (!Files.readAllLines(it).stream().filter(line -> line.equals(metadata)).iterator().hasNext())
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

    public static class ErrorThrowingOutput extends DirectoryOutput {
        public static Config CONFIG = new Config();
        private final Configuration config;

        public ErrorThrowingOutput(Path root, int entriesPerFile, Encoder<String> encoder, Configuration config) {
            super(root, entriesPerFile, encoder, config);
            this.config = config;
        }

        public static class Config extends ConfigurationBase {
            @Override
            public Map<String, String> getDefaults() {
                return DEFAULTS;
            }
            public static class Keys {
                public static final String LOG_OUTPUT_DIR = "test.";
            }
            public static final Map<String, String> DEFAULTS = new HashMap<>() {{
                put(DefaultErrorHandler.Config.Keys.LOG_OUTPUT_DIR, "/tmp/");
            }};
            public boolean throwOn(Configuration config, final String fn){
                return config.containsKey(fn) && config.getBoolean(fn);
            }
        }


        public static ErrorThrowingOutput open(Configuration config) {
            final Encoder encoder =  RuntimeUtil.loadEncoder(config);
            final int entriesPerFile = Integer.valueOf(CONFIG.getOrDefault(config, DirectoryOutput.Config.Keys.ENTRIES_PER_FILE));
            final String outputDirectory = CONFIG.getOrDefault(config, DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY);
            return new ErrorThrowingOutput(Path.of(outputDirectory), entriesPerFile, encoder, config);
        }
        @Override
        public Stream<Optional<CapturedError>> writeVertexStream(Stream<EmittedVertex> vertexStream) {
            return vertexStream.map(it -> {
                return Optional.of(new CapturedError( new Exception("Test exception"), it.id()));
            });
        }

        @Override
        public Stream<Optional<CapturedError>> writeEdgeStream(Stream<EmittedEdge> edgeStream) {
            return null;
        }

        @Override
        public OutputWriter vertexWriter(String label) {
            if(CONFIG.throwOn(config, "test.vertexWriter.throw"))
                throw new RuntimeException("vertexWriter exception");
            return super.vertexWriter(label);
        }

        @Override
        public OutputWriter edgeWriter(String label) {
            if(CONFIG.throwOn(config, "test.edgeWriter.throw"))
                throw new RuntimeException("edgeWriter exception");
            return super.edgeWriter(label);
        }

        @Override
        public Long getEdgeMetric() {
            if(CONFIG.throwOn(config, "test.getEdgeMetric.throw"))
                throw new RuntimeException("getEdgeMetric exception");
            return super.getEdgeMetric();
        }

        @Override
        public Long getVertexMetric() {
            throw new RuntimeException("getVertexMetric exception");
        }

        @Override
        public void close() {
            throw new RuntimeException("close exception");
        }

        @Override
        public void dropStorage() {
            throw new RuntimeException("dropStorage exception");
        }
    }

    @Test
    public void testErrorHandling(){
        testCSVConfiguration.setProperty(ConfigurationBase.Keys.OUTPUT,ErrorThrowingOutput.class.getName());
        final LocalParallelStreamRuntime runtime = new LocalParallelStreamRuntime(testCSVConfiguration);
        runtime.processVertexStream();

    }
}
