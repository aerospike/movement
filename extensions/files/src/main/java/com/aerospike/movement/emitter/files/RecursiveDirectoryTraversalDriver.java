/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.files;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import com.aerospike.movement.util.core.stream.sequence.PotentialSequence;
import com.aerospike.movement.util.core.stream.sequence.SequenceUtil;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RecursiveDirectoryTraversalDriver extends WorkChunkDriver {
    private final Runtime.PHASE phase;
    //Driver Instances for a PipelineGroup must read from the same iterator
    public static Map<Runtime.PHASE, PotentialSequence<?>> phaseSequences = new ConcurrentHashMap<>();
    private static PotentialSequence<?> sequence;

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
            return ConfigUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String DIRECTORY_TO_TRAVERSE = "loader.traversal.directory";
            public static final String DIRECTORY_TO_URI = "loader.traversal.uri";

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};

    }

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private RecursiveDirectoryTraversalDriver(Runtime.PHASE phase, final Configuration config) {
        super(Config.INSTANCE, config);
        this.phase = phase;
    }

    @Override
    protected AtomicBoolean getInitialized() {
        return initialized;
    }

    public static RecursiveDirectoryTraversalDriver open(final Configuration config) {
        final RecursiveDirectoryTraversalDriver x = new RecursiveDirectoryTraversalDriver(RuntimeUtil.getCurrentPhase(config), config);
        x.init(config);
        return x;
    }

    public static Path uriOrFile(String str){
        try {
            return Paths.get(URI.create(str));
        } catch (Exception e) {
            return Path.of(str);
        }

    }
    @Override
    public void init(final Configuration config) {
        synchronized (this) {
            if (!initialized.get()) {
                final Path basePath = uriOrFile(DirectoryEmitter.CONFIG.getOrDefault(DirectoryEmitter.Config.Keys.BASE_PATH, config));

                assert Files.exists(basePath);
                final String phaseOnePath = DirectoryEmitter.CONFIG.getOrDefault(DirectoryEmitter.Config.Keys.PHASE_ONE_SUBDIR, config);
                final String phaseTwoPath = DirectoryEmitter.CONFIG.getOrDefault(DirectoryEmitter.Config.Keys.PHASE_TWO_SUBDIR, config);

                final Path elementTypePath;
                if(config.containsKey(Config.Keys.DIRECTORY_TO_TRAVERSE))
                                elementTypePath = uriOrFile(config.getString(Config.Keys.DIRECTORY_TO_TRAVERSE));
                else
                    elementTypePath =
                                phase.equals(Runtime.PHASE.ONE) ?
                                        Paths.get(basePath.toUri().resolve(phaseOnePath))
                                        : Paths.get(basePath.toUri().resolve(phaseTwoPath));
                if (!phaseSequences.containsKey(phase)) {
                    phaseSequences.put(phase, pathSequence(RuntimeUtil.getCurrentPhase(config), elementTypePath, config));
                }
                this.sequence = phaseSequences.get(phase);

                initialized.set(true);
            }
        }
    }

    @Override
    public void onClose() {
        synchronized (RecursiveDirectoryTraversalDriver.class) {
            if (initialized.compareAndSet(true, false)) {
                phaseSequences.remove(this.phase);
            }
        }
    }


    @Override
    public Optional<WorkChunk> getNext() {
        if (!initialized.get()) {
            throw new IllegalStateException("WorkChunkDriver not initialized");
        }
        return (Optional<WorkChunk>) sequence.getNext();
    }


    private static PotentialSequence<WorkChunk> pathSequence(final Runtime.PHASE phase, final Path elementTypePath, final Configuration config) {
        if (phase.equals(Runtime.PHASE.ONE) || phase.equals(Runtime.PHASE.TWO)) {
            return SequenceUtil.fuse(OneShotIteratorSupplier.of(() -> {
                try {
                    List<Path> fileList = Files.walk(elementTypePath).collect(Collectors.toList());
                    Collections.shuffle(fileList);
                    return fileList.stream()
                            .filter(file -> !Files.isDirectory(file))
                            .map(it -> EmittableWorkChunkFile.from((Path) it, phase, config))
                            .iterator();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        throw new IllegalStateException("Unknown phase " + phase);
    }
}


