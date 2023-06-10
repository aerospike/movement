package com.aerospike.graph.generator.output.file;

import com.aerospike.graph.generator.AbstractGeneratorTest;
import com.aerospike.graph.generator.TestUtil;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.emitter.generated.GeneratedVertex;
import com.aerospike.graph.generator.emitter.generated.VertexContext;
import com.aerospike.graph.generator.runtime.CapturedError;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class DirectoryOutputTest extends AbstractGeneratorTest {
    @Test
    public void testFileSplitting() throws IOException {

        final TestUtil.TestToStringEncoder encoder = new TestUtil.TestToStringEncoder();

        final DirectoryOutput dirOut = new DirectoryOutput(TestUtil.createTempDirectory(), 2, encoder, emptyConfiguration());
        final Path root = dirOut.getRootPath();
        final List<Long> ids = List.of(1L, 2L);
        final VertexContext vertexContext = new VertexContext(testGraphSchema(), testVertexSchema(), ids.iterator());
        final Stream<EmittedVertex> emittedStream = ids.stream().map(it -> (EmittedVertex) new GeneratedVertex(it, vertexContext));
        final Stream<Optional<CapturedError>> errors = dirOut.writeVertexStream(emittedStream);

        errors.filter(Optional::isPresent).map(Optional::get)
                .forEach(System.out::println);
        final List<Path> filesWritten = Files.walk(root).filter(it -> !Files.isDirectory(it)).collect(Collectors.toList());
        System.out.println("Files written: " + filesWritten);
        assertEquals(1, filesWritten.size());
        final String labelA = testVertexSchema().label;
        assertTrue(filesWritten.stream().filter(it -> it.toString().contains(String.format("%s/%s", labelA, labelA))).iterator().hasNext());

    }
}