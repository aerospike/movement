package com.aerospike.graph.generator.format.csv;

import com.aerospike.graph.generator.AbstractGeneratorTest;
import com.aerospike.graph.generator.TestUtil;
import com.aerospike.graph.generator.emitter.generated.GeneratedVertex;
import com.aerospike.graph.generator.emitter.generated.VertexContext;
import com.aerospike.graph.generator.emitter.generated.schema.def.GraphSchema;
import com.aerospike.graph.generator.emitter.generated.schema.def.VertexSchema;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.encoder.format.csv.CSVEncoder;
import com.aerospike.graph.generator.output.file.SplitFileLineOutput;
import com.aerospike.graph.generator.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class CSVEncoderTest extends AbstractGeneratorTest {
    @Test
    public void testWriteCSVLine() throws IOException {
        final Path tempPath = TestUtil.createTempDirectory();
        final Encoder<String> encoder = CSVEncoder.open(new MapConfiguration(new HashMap<>() {{
            put(CSVEncoder.Config.Keys.SCHEMA_FILE, testGraphSchemaLocationRelativeToModule());
        }}));
        Configuration config = RuntimeUtil.loadConfiguration(testGraphSchemaLocationRelativeToModule());
        final String metadata = encoder.encodeVertexMetadata(testVertexSchema().label);
        final SplitFileLineOutput fo = new SplitFileLineOutput("test", tempPath, 1024, encoder, 20, config);
        final GraphSchema graphSchema = testGraphSchema();
        final VertexSchema vertexSchema = testVertexSchema();
        final VertexContext vertexContext = new VertexContext(graphSchema, vertexSchema, IteratorUtils.of(1L));
        GeneratedVertex vertex = new GeneratedVertex(1, vertexContext);
        fo.writeVertex(vertex);
        final Path currentPath = Path.of(fo.getCurrentFile());
        fo.close();
        final List<String> fileData = Files.readAllLines(currentPath);
        fileData.forEach(System.out::println);
        assertTrue(Files.readAllLines(currentPath).stream().filter(it -> it.startsWith("1")).iterator().hasNext());
    }

}
