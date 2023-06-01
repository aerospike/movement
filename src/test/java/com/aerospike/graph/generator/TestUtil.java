package com.aerospike.graph.generator;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.util.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestUtil {
    public static void recursiveDelete(Path path) {
        try {
            Files.walk(path).filter(Files::isRegularFile).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {

        }
    }
    public static Path createTempFile() {
        final Path tempFile;
        try {
            tempFile = Files.createTempFile("test", ".txt");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tempFile.toFile().deleteOnExit();
        return tempFile;
    }

    public static Path createTempDirectory() {
        final Path tempDir;
        try {
            tempDir = Files.createTempDirectory("test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tempDir.toFile().deleteOnExit();
        return tempDir;
    }

    public static String readFromResources(String resourceName) {
        try (InputStream is = IOUtil.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (is == null)
                throw new RuntimeException("Could not find resource: " + resourceName);
            return new String(is.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TestToStringEncoder extends Encoder<String> {

        @Override
        public String encodeEdge(final EmittedEdge edge) {
            return edge.toString();
        }

        @Override
        public String encodeVertex(final EmittedVertex vertex) {
            return vertex.toString();
        }

        @Override
        public String encodeVertexMetadata(final EmittedVertex vertex) {
            return "";
        }

        @Override
        public String encodeEdgeMetadata(final EmittedEdge edge) {
            return "";
        }

        @Override
        public String encodeVertexMetadata(final String label) {
            return "";
        }

        @Override
        public String encodeEdgeMetadata(final String label) {
            return "";
        }


        @Override
        public String getExtension() {
            return "";
        }

        @Override
        public void close() {

        }
    }

    public static class TestEmittedIdImpl {
        private final Long id;

        public TestEmittedIdImpl(final Long id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return String.valueOf(id);
        }
    }

    public static class Schema {
        public static class Vertex {
            public static final String ACCOUNT = "Account";
        }
    }
}

