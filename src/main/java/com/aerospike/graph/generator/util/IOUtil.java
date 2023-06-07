package com.aerospike.graph.generator.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

public final class IOUtil {
    private IOUtil() {
    }

    public static void copyResourceToDirectory(String resourceName, Path filePath) {
        try (InputStream is = IOUtil.class.getClassLoader().getResourceAsStream(resourceName)) {
            Files.copy(is, filePath.resolve(resourceName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void loadKryoDataFromResources(GraphTraversalSource g, String resourceName) {
        final Path tempPath;
        try {
            tempPath = Files.createTempDirectory("generator-test").toAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tempPath.toFile().deleteOnExit();
        IOUtil.copyResourceToDirectory(resourceName, tempPath);
        String resourcePath = tempPath.resolve(resourceName).toAbsolutePath().toString();
        g.io(resourcePath).read().iterate();
    }

    public static void loadGraphmlFromData(Graph graph, String resourceName) throws IOException {
        Path dataPath = Path.of("../data");
        String resourcePath = dataPath.resolve(resourceName).toAbsolutePath().toString();
        graph.io(graphml()).readGraph(resourcePath);
    }

    public static void downloadFileFromURL(URL source, File dest) {
        try (BufferedInputStream in = new BufferedInputStream(source.openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(dest)) {
            byte dataBuffer[] = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static long calculateDirectorySize(String directoryPath) throws IOException {
        Path path = Paths.get(directoryPath);
        SizeVisitor visitor = new SizeVisitor();
        Files.walkFileTree(path, visitor);

        return visitor.getTotalSize();
    }

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

    private static class SizeVisitor extends SimpleFileVisitor<Path> {
        private long totalSize;

        public long getTotalSize() {
            return totalSize;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            totalSize += attrs.size();
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            // Print the error, or handle it as per your requirements
            System.err.println("Failed to access file: " + file + ". Error: " + exc);
            return FileVisitResult.CONTINUE;
        }
    }


}