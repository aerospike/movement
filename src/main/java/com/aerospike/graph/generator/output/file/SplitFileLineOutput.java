package com.aerospike.graph.generator.output.file;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;
import com.aerospike.graph.generator.encoder.Encoder;
import com.aerospike.graph.generator.output.OutputWriter;
import org.apache.commons.configuration2.Configuration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * <p>
 * Write should return True if more lines are available in the file
 * it should return False if no more lines are available in the file
 */
public class SplitFileLineOutput implements OutputWriter {
    private final int writesBeforeFlush;
    private final Encoder encoder;
    private final long maxLines;
    private final Path basePath;
    private final String name;
    private final AtomicLong metric;
    private final Configuration config;
    boolean closed = false;
    FileWriter fileWriter;
    BufferedWriter bufferedWriter;
    int writeCountSinceLastFlush = 0;
    private String outputFile = null;
    private AtomicLong linesWritten = new AtomicLong(0);
    private static AtomicLong fileIncr = new AtomicLong(0);

    /**
     * 8m buffer
     * we flush at 8m or at writesBeforeFlush, whichever comes first
     */
    final int bufferSize; // 8M

    public SplitFileLineOutput(final String name,
                               final Path basePath,
                               final int writesBeforeFlush,
                               final Encoder<String> encoder,
                               final long maxLines,
                               final AtomicLong metric,
                               final Configuration config) {
        this.config = config;
        this.name = name;
        this.basePath = basePath;
        this.maxLines = maxLines;
        this.encoder = encoder;
        this.writesBeforeFlush = writesBeforeFlush;
        this.metric = metric;
        this.closed = true;
        this.bufferSize = Integer.parseInt(DirectoryOutput.CONFIG.getOrDefault(config, DirectoryOutput.Config.Keys.BUFFER_SIZE_KB)) * 1024;
    }

    public SplitFileLineOutput(final String name,
                               final Path basePath,
                               final int writesBeforeFlush,
                               final Encoder<String> encoder,
                               final long maxLines,
                               final Configuration config) {
        this(name, basePath, writesBeforeFlush, encoder, maxLines, new AtomicLong(0), config);
    }

    public AtomicLong getMetric() {
        return metric;
    }

    public void writeVertexLine(final EmittedVertex vertex) {
        write(encoder.encodeVertex(vertex) + "\n", (String) encoder.encodeVertexMetadata(vertex));
    }

    public void writeEdgeLine(final EmittedEdge edge) {
        write(encoder.encodeEdge(edge) + "\n", (String) encoder.encodeEdgeMetadata(edge));
    }

    public void write(final String string, final String header) {
        if (closed) {
            createNewFile(header);
        }
        try {
            bufferedWriter.write(string);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (++writeCountSinceLastFlush >= writesBeforeFlush) {
            flush();
        }
        if (linesWritten.incrementAndGet() >= maxLines) {
            closeFile();
        }
        metric.addAndGet(1);
    }

    // Just for testing.
    public String getCurrentFile() {
        return outputFile;
    }

    private void createNewFile(final String header) {
        try {
            if (!basePath.toFile().exists()) {
                basePath.toFile().mkdirs();
            }
            outputFile = basePath.resolve(String.format("%s_%d.%s", name, fileIncr.incrementAndGet(), ((Encoder) encoder).getExtension())).toString();
            final File file = new File(outputFile);
            //LOG.debug(file);
            // if we are appending, we don't want to overwrite the file
            fileWriter = new FileWriter(file, false);
            bufferedWriter = new BufferedWriter(fileWriter, bufferSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        linesWritten.set(0);
        this.closed = false;
        try {
            bufferedWriter.write(header + "\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void closeFile() {
        closed = true;
        flush();
        try {
            bufferedWriter.close();
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeEdge(final EmittedEdge edge) {
        writeEdgeLine(edge);
    }

    @Override
    public void writeVertex(final EmittedVertex vertex) {
        writeVertexLine(vertex);
    }


    @Override
    public void init() {
    }

    @Override
    public void close() {
        flush();
        closed = true;
    }

    @Override
    public void flush() {
        try {
            if (!closed) {
                bufferedWriter.flush();
                writeCountSinceLastFlush = 0;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return String.format("SplitFileLineOutput: %s", basePath.toString());
    }
}