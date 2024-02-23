/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.output.files;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.output.core.OutputWriter;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import org.apache.commons.configuration2.Configuration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * <p>
 * Write should return True if more lines are available in the file
 * it should return False if no more lines are available in the file
 */
public class SplitFileLineOutput implements OutputWriter {


    private final String label;
    private final Semaphore outstanding;

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigUtil.getKeysFromClass(DirectoryOutput.Config.Keys.class);
        }


        public static class Keys {
            public static final String BUFFER_SIZE_KB = "output.bufferSizeKB";
            public static final String WRITES_BEFORE_FLUSH = "output.writesBeforeFlush";
            public static final String MAX_LINES = "output.entriesPerFile";
            public static final String DIRECTORY = DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY;

            public static final String EXTENSION = "output.file.extension";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Config.Keys.MAX_LINES, "1000");
            put(Config.Keys.BUFFER_SIZE_KB, "4096");
            put(Config.Keys.WRITES_BEFORE_FLUSH, "1000");
            put(Config.Keys.DIRECTORY, DirectoryOutput.Config.INSTANCE.defaultConfigMap().get(DirectoryOutput.Config.Keys.OUTPUT_DIRECTORY));
        }};
    }


    static final AtomicLong fileIncr = new AtomicLong(0);
    private final int writesBeforeFlush;
    private final Encoder encoder;
    private final long maxLines;
    private final Path basePath;
    private final AtomicLong metric;
    private final Configuration config;
    boolean closed = false;
    private FileWriter fileWriter;
    private BufferedWriter bufferedWriter;
    private int writeCountSinceLastFlush = 0;
    private AtomicLong linesWritten = new AtomicLong(0);

    /**
     * 8m buffer
     * we flush at 8m or at writesBeforeFlush, whichever comes first
     */
    final int bufferSize; // 8M

    private SplitFileLineOutput(
            final String label,
            final Encoder<String> encoder,
            final AtomicLong metric,
            final Configuration config) {
        this.label = label;
        this.config = config;
        this.encoder = encoder;
        this.basePath = Path.of((String) (Config.INSTANCE.getOrDefault(Config.Keys.DIRECTORY, config)));
        this.maxLines = Long.parseLong(Config.INSTANCE.getOrDefault(Config.Keys.MAX_LINES, config));
        this.writesBeforeFlush = Integer.parseInt(Config.INSTANCE.getOrDefault(Config.Keys.WRITES_BEFORE_FLUSH, config));
        this.metric = metric;
        this.closed = true;
        this.bufferSize = Integer.parseInt(DirectoryOutput.CONFIG.getOrDefault(DirectoryOutput.Config.Keys.BUFFER_SIZE_KB, config)) * 1024;
        this.outstanding = new Semaphore(bufferSize);
    }

    public static SplitFileLineOutput create(final String label, final Encoder<String> encoder, final AtomicLong metric, final Configuration config) {
        return new SplitFileLineOutput(label, encoder, metric, config);
    }

    @Override
    public void writeToOutput(final Optional<Emitable> potentialEmitable) {
        if (potentialEmitable.isPresent()) {
            try {
                outstanding.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Emitable emitable = potentialEmitable.get();
            writeEmitable(emitable);
        }
    }


    @Override
    public void init() {
    }





    public AtomicLong getMetric() {
        return metric;
    }


    public void writeEmitable(final Emitable item) {
        try {
            final String header = (String) encoder.encodeItemMetadata(item).orElseThrow(() -> new RuntimeException("No metadata for " + item));
            Optional encoded = encoder.encode(item);
            if (encoded.isPresent()) {
                write(encoded.get() + "\n", header);
                outstanding.release();
            } else {
                throw new RuntimeException("could not encode item: " + item);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
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

    private String nextFileName() {
        return basePath.resolve(label).resolve(String.format("%s_%d.%s", label, fileIncr.incrementAndGet(),
                ((Encoder) encoder).getEncoderMetadata().getOrDefault(Config.Keys.EXTENSION, "csv"))).toString();
    }

    private void createNewFile(final String header) {
        try {
            if (!basePath.toFile().exists()) {
                basePath.toFile().mkdirs();
            }
            final File file = new File(nextFileName());
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

    @Override
    public void flush() {
        try {
            bufferedWriter.flush();
            fileWriter.flush();
            writeCountSinceLastFlush = 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void close() {
        System.out.println("closing " + SplitFileLineOutput.class.getSimpleName() + ": " + this);
        try {
            outstanding.acquire(bufferSize);
            outstanding.release(bufferSize);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        closeFile();
        closed = true;
    }

    private void closeFile() {
        synchronized (this) {
            closed = true;
            flush();
            try {
                bufferedWriter.close();
                fileWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    public String toString() {
        return String.format("SplitFileLineOutput: path: %s availablePermits: %d, bufferSize: %d", basePath.toString(), outstanding.availablePermits(), bufferSize);
    }
}
