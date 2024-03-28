/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.files;

import com.aerospike.movement.output.files.DirectoryOutput;
import com.aerospike.movement.output.files.SplitFileLineOutput;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.test.core.AbstractMovementTest;
import com.aerospike.movement.test.mock.MockUtil;
import com.aerospike.movement.test.mock.emitter.MockEmitable;
import com.aerospike.movement.test.mock.encoder.MockEncoder;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.runtime.IOUtil;
import com.aerospike.movement.util.files.FileUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.movement.emitter.core.Emitter.encodeToOutput;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDirectoryWriter extends AbstractMovementTest {
    final Path outputDirectory = IOUtil.createTempDir();

    @Before
    public void cleanDirectory() {
        FileUtil.recursiveDelete(outputDirectory);
    }

    @Test
    public void testWriteFile() throws IOException {
        FileUtil.recursiveDelete(outputDirectory);
        MockUtil.setDefaultMockCallbacks();
        final Configuration testConfig = getMockConfiguration(new HashMap<>() {{
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, outputDirectory.toAbsolutePath().toString());
        }});

        final MockEncoder<String> enc = MockEncoder.open(testConfig);

        final AtomicLong fileMetric = new AtomicLong(0L);
        final String valueToWrite = "a";
        final String headerToWrite = "b";
        outputDirectory.toFile().mkdir();
        outputDirectory.resolve("test").toFile().mkdir();
        SplitFileLineOutput.fileIncr.set(0);
        final SplitFileLineOutput x = SplitFileLineOutput.create("test", enc, fileMetric, testConfig);
        x.write(valueToWrite, headerToWrite);
        x.close();
        assertEquals(1, Files.walk(outputDirectory).filter(it -> it.toString().endsWith(".csv")).count());
        assertEquals(1, Files.lines(outputDirectory.resolve("test").resolve("test_1.csv"))
                .filter(line -> line.contains(valueToWrite)).count());
    }

    @Test
    public void testWriteDirectory() throws IOException {
        FileUtil.recursiveDelete(outputDirectory);

        MockUtil.setDefaultMockCallbacks();
        final Configuration testConfig = getMockConfiguration(new HashMap<>() {{
            put(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY, outputDirectory.toAbsolutePath().toString());
        }});

        MockEmitable emitable = new MockEmitable("z", true, ConfigUtil.empty());
        SplitFileLineOutput.fileIncr.set(0);
        final DirectoryOutput o = DirectoryOutput.open(testConfig);

        o.writer(MockEmitable.class, "b").writeToOutput(Optional.of(emitable));
        o.close();
        assertEquals(1, Files.walk(outputDirectory).filter(it -> it.toString().endsWith(".csv")).count());
        Files.walk(outputDirectory).forEach(System.out::println);
        assertTrue(outputDirectory.resolve("mock").toFile().isDirectory());
        assertTrue(outputDirectory.resolve("mock").resolve("b").toFile().isDirectory());
        assertTrue(outputDirectory.resolve("mock").resolve("b").resolve("b_1.csv").toFile().exists());
        Files.walk(outputDirectory).forEach(System.out::println);
    }

    public static long countLinesInDirectory(Path path) throws IOException {
        return Files.walk(path)
                .filter(it -> it.toFile().exists() && it.toFile().isFile())
                .flatMap(it -> {
                    try {
                        return Files.lines(it);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).count();
    }

    public static long countFilesInDirectory(Path path) throws IOException {
        return Files.walk(path)
                .filter(it -> it.toFile().exists() && it.toFile().isFile())
                .count();
    }

    @Test
    public void testEmitToDirectory() throws IOException {
        FileUtil.recursiveDelete(outputDirectory);
        FileTestUtil.writeClassicGraphToDirectory(outputDirectory);
        assertTrue(outputDirectory.resolve("edges").toFile().isDirectory());
        assertTrue(outputDirectory.resolve("vertices").toFile().isDirectory());
        assertEquals(2, Files.list(outputDirectory).count());
        long totalVertexLines = countLinesInDirectory(outputDirectory.resolve("vertices"));
        long vertexFileCount = countFilesInDirectory(outputDirectory.resolve("vertices"));
        long totalEdgeLines = countLinesInDirectory(outputDirectory.resolve("edges"));
        long edgeFileCount = countFilesInDirectory(outputDirectory.resolve("edges"));
        long vertexHeaderCount = vertexFileCount;
        long edgeHeaderCount = edgeFileCount;

        assertEquals(TinkerFactory.createClassic().traversal().V().count().next() + vertexHeaderCount, totalVertexLines);
        assertEquals(TinkerFactory.createClassic().traversal().E().count().next() + edgeHeaderCount, totalEdgeLines);
        assertEquals(6, Files.walk(outputDirectory).filter(it -> it.toFile().exists() && it.toFile().isFile()).count());
    }
}
