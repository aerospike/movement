/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.util.files;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;

import static com.aerospike.movement.emitter.files.RecursiveDirectoryTraversalDriver.uriOrFile;

public class FileUtil {

    public static void copyResourceToDirectory(String resourceName, Path filePath) {
        try (InputStream is = FileUtil.class.getClassLoader().getResourceAsStream(resourceName)) {
            Files.copy(is, filePath.resolve(resourceName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static long calculateDirectorySize(String directoryPath) throws IOException {
        final Path path = uriOrFile(directoryPath);
        final SizeVisitor visitor = new SizeVisitor();
        Files.walkFileTree(path, visitor);
        return visitor.getTotalSize();
    }

    public static void recursiveDelete(Path path) {
        if (path.toFile().exists()) {
            try {
                Files.walk(path)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
