/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.files;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkChunkId;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import org.apache.commons.configuration2.Configuration;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class EmittableWorkChunkFile implements WorkChunk, Emitable {
    private final UUID uuid;
    private final Configuration config;
    private final Iterator<String> iterator;
    private final String header;
    private final Runtime.PHASE phase;
    private AtomicLong lineCounter = new AtomicLong(0);
    private final Decoder<String> decoder;
    private Path filePath;

    public EmittableWorkChunkFile(final Path filePath,
                                  final String header,
                                  final Iterator<String> iterator,
                                  final Runtime.PHASE phase,
                                  final Decoder<String> decoder,
                                  final Configuration config) {
        this.filePath = filePath;
        this.header = header;
        this.config = config;
        this.uuid = UUID.randomUUID();
        this.iterator = iterator;
        this.decoder = decoder;
        this.phase = phase;
    }

    public static WorkChunk from(final Path filePath, Runtime.PHASE phase, final Configuration config) {
        final Iterator<String> iterator;
        final String header;
        final Decoder<String> decoder = (Decoder<String>) RuntimeUtil.lookupOrLoad(Decoder.class, config);
        try {
            iterator = Files.lines(filePath).iterator();
            header = iterator.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new EmittableWorkChunkFile(filePath, header, iterator, phase, decoder, config);
    }

    @Override
    public WorkChunkId next() {
        throw new IllegalStateException(EmittableWorkChunkFile.class + " is passthrough");
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public UUID getId() {
        return uuid;
    }

    public Path getPath() {
        return filePath;
    }

    @Override
    public Stream<Emitable> emit(final Output unused) {
        return stream();
    }

    public Stream<Emitable> stream() {
        return IteratorUtils.stream(iterator)
                .map(it -> decoder.decodeElement(it, header, phase));
    }

    @Override
    public String type() {
        return filePath.getParent().getFileName().toString();
    }


    public static class WorkFileEntryId extends WorkChunkId {
        public final Path filePath;
        public final String fileLine;

        public WorkFileEntryId(final Long id, final Path filePath, final String line) {
            super(new AbstractMap.SimpleEntry<>(filePath, id));
            this.filePath = filePath;
            this.fileLine = line;
        }

        @Override
        public Map.Entry<Path, Long> getId() {
            return (Map.Entry<Path, Long>) id;
        }
    }
}
