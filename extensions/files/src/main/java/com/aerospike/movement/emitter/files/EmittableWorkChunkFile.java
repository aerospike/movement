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
import com.aerospike.movement.runtime.core.driver.WorkItem;
import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.stream.sequence.PotentialSequence;
import com.aerospike.movement.util.core.stream.sequence.SequenceUtil;
import org.apache.commons.configuration2.Configuration;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class EmittableWorkChunkFile implements WorkChunk, Emitable {
    private final UUID uuid;
    private final Configuration config;
    private final PotentialSequence<String> pse;
    private final String header;
    private final Runtime.PHASE phase;
    private AtomicLong lineCounter = new AtomicLong(0);
    private final Decoder<String> decoder;
    private Path filePath;

    public EmittableWorkChunkFile(final Path filePath,
                                  final String header,
                                  final PotentialSequence<String> pse,
                                  final Runtime.PHASE phase,
                                  final Decoder<String> decoder,
                                  final Configuration config) {
        this.filePath = filePath;
        this.header = header;
        this.config = config;
        this.uuid = UUID.randomUUID();
        this.pse = pse;
        this.decoder = decoder;
        this.phase = phase;
    }

    public static WorkChunk from(final Path filePath, Runtime.PHASE phase, final Configuration config) {
        final PotentialSequence<String> pse;
        final String header;
        try {
            header = Files.lines(filePath).iterator().next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final Decoder<String> decoder = (Decoder<String>) RuntimeUtil.lookupOrLoad(Decoder.class, config);
        pse = SequenceUtil.fuse(OneShotIteratorSupplier.of(() -> {
            try {
                final Iterator<String> i = Files.lines(filePath).iterator();
                i.next(); // skip header
                return i;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
        return new EmittableWorkChunkFile(filePath, header, pse, phase, decoder, config);
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
        return pse.stream().filter(it -> it.isPresent()).map(it -> it.get()).map(it -> decoder.decodeElement(it, header, phase));
    }

    @Override
    public Optional<WorkItem> getNext() {
        throw new IllegalStateException(EmittableWorkChunkFile.class + " is passthrough");
    }


    @Override
    public String type() {
        return filePath.getParent().getFileName().toString();
    }


}
