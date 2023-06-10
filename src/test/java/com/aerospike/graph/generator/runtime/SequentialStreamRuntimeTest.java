package com.aerospike.graph.generator.runtime;

import com.aerospike.graph.generator.AbstractGeneratorTest;
import com.aerospike.graph.generator.TestUtil;
import com.aerospike.graph.generator.emitter.Emitter;
import com.aerospike.graph.generator.emitter.generated.Generator;
import com.aerospike.graph.generator.emitter.generated.StitchMemory;
import com.aerospike.graph.generator.emitter.generated.schema.Parser;
import com.aerospike.graph.generator.emitter.generated.schema.def.GraphSchema;
import com.aerospike.graph.generator.emitter.generated.schema.def.VertexSchema;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.encoder.format.csv.CSVEncoder;
import com.aerospike.graph.generator.encoder.format.tinkerpop.GraphEncoder;
import com.aerospike.graph.generator.encoder.format.tinkerpop.TraversalEncoder;
import com.aerospike.graph.generator.output.Output;
import com.aerospike.graph.generator.output.file.DirectoryOutput;
import com.aerospike.graph.generator.output.graph.GraphOutput;
import com.aerospike.graph.generator.output.graph.TraversalOutput;
import com.aerospike.graph.generator.util.ConfigurationBase;
import com.aerospike.graph.generator.util.IOUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.aerospike.graph.generator.emitter.generated.Generator.Config.Keys.ROOT_VERTEX_ID_END;
import static com.aerospike.graph.generator.output.file.DirectoryOutput.Config.Keys.ENTRIES_PER_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class SequentialStreamRuntimeTest extends AbstractGeneratorTest {
    Configuration testConfiguration;

    @Before
    public void setup() {
        testConfiguration = ConfigurationBase.getCSVSampleConfiguration(newGraphSchemaLocationRelativeToModule(), TestUtil.createTempDirectory().toString());
    }

    @Test
    public void writeToGraphTest() {
        final StitchMemory stitchMemory = new StitchMemory("none");
        final Graph graph = TinkerGraph.open();
        final Encoder encoder = new GraphEncoder(graph);
        final Output graphOutput = new GraphOutput((GraphEncoder) encoder);
        final Emitter emitter = Generator.open(testConfiguration);
        final LocalSequentialStreamRuntime runtime = new LocalSequentialStreamRuntime(testConfiguration, stitchMemory, Optional.of(graphOutput), Optional.of(emitter));
        runtime.processVertexStream();
        final List<Vertex> allV = graph.traversal().V().toList();
        final Integer entriesPerFile = Integer.parseInt(testConfiguration.getString(ENTRIES_PER_FILE));
        final Integer rootVertexIdEnd = Integer.parseInt(testConfiguration.getString(ROOT_VERTEX_ID_END));
        // 1 Account = 5 Devices, 6 vertices/vertexIdEnd.
        assertEquals(8 * rootVertexIdEnd, allV.size());
        final GraphSchema newSchema = Parser.parse(Path.of(newGraphSchemaLocationRelativeToModule()));
        final VertexSchema rootVertexSchema = newSchema.vertexTypes.stream().filter(v -> v.label.equals(newSchema.entrypointVertexType)).findFirst().get();
        assertEquals(rootVertexIdEnd.longValue(), graph.traversal().V().hasLabel(rootVertexSchema.label).count().next().longValue());
        assertEquals((Long) (7L * rootVertexIdEnd), (Long) graphOutput.getEdgeMetric());
        assertEquals((Long) (8L * rootVertexIdEnd), (Long) graphOutput.getVertexMetric());
    }

    @Test
    @Ignore // Ignore because requires remote graph to be running.
    public void writeToRemoteGraphTest() {
        final Configuration remoteConfiguration = ConfigurationBase.getRemoteSampleConfiguration(newGraphSchemaLocationRelativeToModule());
        final StitchMemory stitchMemory = new StitchMemory("none");
        final Output graphOutput = TraversalOutput.open(remoteConfiguration);
        final Emitter emitter = Generator.open(remoteConfiguration);
        final LocalSequentialStreamRuntime runtime = new LocalSequentialStreamRuntime(remoteConfiguration, stitchMemory, Optional.of(graphOutput), Optional.of(emitter));
        runtime.processVertexStream();

        final String host = TraversalEncoder.CONFIG.getOrDefault(remoteConfiguration, TraversalEncoder.Config.Keys.HOST);
        final String port = TraversalEncoder.CONFIG.getOrDefault(remoteConfiguration, TraversalEncoder.Config.Keys.PORT);
        final String remoteTraversalSourceName = TraversalEncoder.CONFIG.getOrDefault(remoteConfiguration, TraversalEncoder.Config.Keys.REMOTE_TRAVERSAL_SOURCE_NAME);
        GraphTraversalSource g = AnonymousTraversalSource
                .traversal()
                .withRemote(DriverRemoteConnection
                        .using(host, Integer.parseInt(port), remoteTraversalSourceName));
        final List<Vertex> allV = g.V().toList();
        // 1 GoldenEntity -> 1 DigitalEntity, 1 ContactMedium, Individual
        // 1 DigitalEntity -> 1 IP, 1 Cookie
        // 1 ContactMedium -> None
        // 1 Individual -> 1 IP, Household
        // 1 Cookie -> None
        // 1 IP -> None
        // 1 Household -> None
        // 1 GoldenEntity => 8 vertices and 7 edges.
        final Integer rootVertexIdEnd = Integer.parseInt(remoteConfiguration.getString(ROOT_VERTEX_ID_END));
        assertEquals(8 * rootVertexIdEnd, allV.size());
        assertEquals(rootVertexIdEnd.longValue(), g.V().hasLabel(emitter.getRootVertexSchema().label).count().next().longValue());
        assertEquals((Long) (7L * rootVertexIdEnd), (Long) graphOutput.getEdgeMetric());
        assertEquals((Long) (7L * rootVertexIdEnd), (Long) graphOutput.getEdgeMetric());
        assertEquals((Long) (8L * rootVertexIdEnd), (Long) graphOutput.getVertexMetric());
    }

    @Test
    public void writeToCSVTest() throws IOException {
        final long startTime = System.currentTimeMillis();
        final StitchMemory stitchMemory = new StitchMemory("none");
        final Encoder<String> encoder = CSVEncoder.open(testConfiguration);
        final Output csvOutput = DirectoryOutput.open(testConfiguration);
        final Emitter emitter = Generator.open(testConfiguration);
        final LocalSequentialStreamRuntime runtime = new LocalSequentialStreamRuntime(testConfiguration, stitchMemory, Optional.of(csvOutput), Optional.of(emitter));
        runtime.processVertexStream();

        csvOutput.close();
        final long stopTime = System.currentTimeMillis();
        System.out.println(csvOutput);
        final Integer entriesPerFile = Integer.parseInt(testConfiguration.getString(ENTRIES_PER_FILE));
        final Integer rootVertexIdEnd = Integer.parseInt(testConfiguration.getString(ROOT_VERTEX_ID_END));

        final Path directory = Path.of(testConfiguration.getString(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY));
        final List<Path> vertexFiles = Files.walk(directory.resolve("vertices")).filter(Files::isRegularFile).collect(Collectors.toList());
        final List<Path> edgeFiles = Files.walk(directory.resolve("edges")).filter(Files::isRegularFile).collect(Collectors.toList());

        // 1 Account -> 5 Device.
        // Therefore, we have rootVertexIdEnd / entriesPerFile files * 6 (5 device + 1 account) vertex files
        // and rootVertexIdEnd / entriesPerFile * 5 edge files
        assertEquals(8 * (rootVertexIdEnd / entriesPerFile), vertexFiles.size());
        assertEquals(7 * (rootVertexIdEnd / entriesPerFile), edgeFiles.size());

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
        // 1 Account -> 5 Device.
        // Therefore, we have rootVertexIdEnd / entriesPerFile files * 6 (5 device + 1 account) vertex files
        // and rootVertexIdEnd / entriesPerFile * 5 edge files
//        assertEquals(8008, vertexEntries);
//        assertEquals(7007, edgeEntries);


        Files.walk(directory.resolve("vertices")).filter(Files::isRegularFile).forEach(it -> {
            String label = it.getFileName().toString().split("_")[0];
            final String labelHeader = encoder.encodeVertexMetadata(label);
            try {
                if (!Files.readAllLines(it).stream().filter(line -> line.equals(labelHeader)).iterator().hasNext())
                    throw new RuntimeException("Missing header for " + it.getFileName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });
        final long totalTime = stopTime - startTime;
        final long writtenSize = IOUtil.calculateDirectorySize(directory.toAbsolutePath().toString());
        System.out.println("Total time: " + totalTime / 1000 + "s");
        System.out.println("Total size: " + writtenSize / 1024 / 1024 + " MB");
        System.out.println("MB/s: " + (writtenSize / totalTime) / 1000);
        System.out.println("Records/s: " + (csvOutput.getVertexMetric() + csvOutput.getEdgeMetric()) / totalTime * 1000);
        // To get total count multiply by file count and 100 + 1 (1 added for the header).
//        assertEquals((rootVertexIdEnd / entriesPerFile) * 7 * 1000, csvOutput.getEdgeMetric().get());
//        assertEquals((rootVertexIdEnd / entriesPerFile) * 8 * 1000, csvOutput.getVertexMetric().get());
    }
}
