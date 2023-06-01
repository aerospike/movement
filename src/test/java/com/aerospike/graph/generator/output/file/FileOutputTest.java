package com.aerospike.graph.generator.output.file;

import com.aerospike.graph.generator.AbstractGeneratorTest;
import com.aerospike.graph.generator.TestUtil;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FileOutputTest extends AbstractGeneratorTest {

    @Test
    public void testWriteLine() throws IOException {
        final Path tempPath = TestUtil.createTempDirectory();
        final TestUtil.TestToStringEncoder encoder = new TestUtil.TestToStringEncoder();
        final SplitFileLineOutput fo = new SplitFileLineOutput(
                "test",
                tempPath,
                1024,
                encoder,
                20,
                emptyConfiguration());


        fo.write("test", "test");
        final Path outputFile = Path.of(fo.getCurrentFile());
        fo.close();
        assertTrue(Files.readAllLines(outputFile).contains("test"));
    }

    @Test
    public void testFlushBuffer() throws IOException {
        final int bufferSize = 6;
        final Path tempPath = TestUtil.createTempDirectory();
        final TestUtil.TestToStringEncoder encoder = new TestUtil.TestToStringEncoder();
        final SplitFileLineOutput fo = new SplitFileLineOutput("test", tempPath, bufferSize + 1, encoder, 20, emptyConfiguration());
        final String testHeader = "testHeader";
        final String testString = "test";
        // Write test to buffer.
        fo.write(testString + "\n", testHeader);
        final Path outputPath = Path.of(fo.getCurrentFile());
        // test should not be in output file yet.
        assertFalse(Files.readAllLines(outputPath).contains(testHeader));
        assertFalse(Files.readAllLines(outputPath).contains(testString));

        // Flush buffer to file.
        fo.flush();

        // test should now be in output file.
        assertTrue(Files.readAllLines(outputPath).contains("testHeader"));
        assertTrue(Files.readAllLines(outputPath).contains("test"));

        // For each number in the buffer size, write it to the buffer.
        IntStream.range(0, bufferSize).forEach(i -> fo.write(String.valueOf(i) + "\n", ""));
        // This data should not appear yet.
        assertFalse(Files.readAllLines(outputPath).contains(String.valueOf(bufferSize - 1)));
        fo.flush();
        assertTrue(Files.readAllLines(outputPath).contains(String.valueOf(bufferSize - 1)));

        // Write another line to the buffer and flush it.
        fo.write(String.valueOf(bufferSize) + "\n", "");
        fo.flush();

        // Read file back and check it contains the value just written.
        final List<String> x = Files.readAllLines(outputPath);
        assertTrue(x.contains(String.valueOf(bufferSize)));

        // testHeader, test, 0, 1, ... bufferSize => bufferSize + 3
        assertEquals(bufferSize + 3, Files.readAllLines(outputPath).size());
    }

    @Test
    public void testClose() throws IOException {
        final Path tempPath = TestUtil.createTempDirectory();
        final TestUtil.TestToStringEncoder encoder = new TestUtil.TestToStringEncoder();
        final SplitFileLineOutput fo = new SplitFileLineOutput("test", tempPath, 1024, encoder, 20, emptyConfiguration());
        fo.write("test", "");
        final Path outputFile = Path.of(fo.getCurrentFile());
        fo.close();
        assertTrue(Files.readAllLines(outputFile).contains("test"));

        // Should create a new file when close->write flow happens.
        fo.write("test2", "");
        fo.flush();
        final Path newOutputFile = Path.of(fo.getCurrentFile());
        assertNotEquals(outputFile.toString(), newOutputFile.toString());
        assertFalse(Files.readAllLines(newOutputFile).contains("test"));
        assertTrue(Files.readAllLines(newOutputFile).contains("test2"));
    }

    @Test
    public void willCloseOnMaxLines() throws IOException {
        final Path tempPath = TestUtil.createTempDirectory();
        final TestUtil.TestToStringEncoder encoder = new TestUtil.TestToStringEncoder();
        final SplitFileLineOutput fo = new SplitFileLineOutput("test", tempPath, 1024, encoder, 1, emptyConfiguration());
        fo.write("test" + "\n", "");
        final Path outputFile1 = Path.of(fo.getCurrentFile());
        fo.flush();
        fo.write("test2" + "\n", "");
        fo.flush();
        final Path outputFile2 = Path.of(fo.getCurrentFile());
        assertNotEquals(outputFile1.toString(), outputFile2.toString());
        final List<String> f1Lines = Files.readAllLines(outputFile1);
        final List<String> f2Lines = Files.readAllLines(outputFile2);
        assertTrue(f1Lines.contains("test"));
        assertFalse(f1Lines.contains("test2"));
        assertTrue(f2Lines.contains("test2"));
        assertFalse(f2Lines.contains("test"));
    }
}
