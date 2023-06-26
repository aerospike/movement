package com.aerospike.graph.move.format.csv;

import com.aerospike.graph.move.AbstractGeneratorTest;
import com.aerospike.graph.move.TestUtil;
import com.aerospike.graph.move.emitter.generator.GeneratedVertex;
import com.aerospike.graph.move.emitter.generator.VertexContext;
import com.aerospike.graph.move.emitter.generator.schema.def.GraphSchema;
import com.aerospike.graph.move.emitter.generator.schema.def.VertexSchema;
import com.aerospike.graph.move.encoding.Encoder;
import com.aerospike.graph.move.encoding.format.csv.GraphCSV;
import com.aerospike.graph.move.output.file.SplitFileLineOutput;
import com.aerospike.graph.move.util.RuntimeUtil;
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
        final Encoder<String> encoder = GraphCSV.open(new MapConfiguration(new HashMap<>() {{
            put(GraphCSV.Config.Keys.SCHEMA_FILE, testGraphSchemaLocationRelativeToModule());
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
