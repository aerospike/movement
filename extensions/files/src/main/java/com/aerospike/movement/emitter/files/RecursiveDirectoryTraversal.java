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
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RecursiveDirectoryTraversal extends WorkChunkDriver {
    private final Runtime.PHASE phase;

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
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String DIRECTORY_TO_TRAVERSE = "loader.traversal.directory";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};

    }

    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static Iterator<Path> iterator;

    private RecursiveDirectoryTraversal(Runtime.PHASE phase, final Configuration config) {
        super(Config.INSTANCE, config);
        this.phase = phase;
    }

    @Override
    protected AtomicBoolean getInitialized() {
        return initialized;
    }

    public static RecursiveDirectoryTraversal open(final Configuration config) {
        return new RecursiveDirectoryTraversal(RuntimeUtil.getCurrentPhase(config), config);
    }

    @Override
    public void init(final Configuration config) {
        synchronized (RecursiveDirectoryTraversal.class) {
            if (!initialized.get()) {
                final String baseDir = DirectoryLoader.CONFIG.getOrDefault(DirectoryLoader.Config.Keys.BASE_PATH, config);
                final Path basePath = Path.of(baseDir);
                final String phaseOnePath = DirectoryLoader.CONFIG.getOrDefault(DirectoryLoader.Config.Keys.PHASE_ONE_SUBDIR, config);
                final String phaseTwoPath = DirectoryLoader.CONFIG.getOrDefault(DirectoryLoader.Config.Keys.PHASE_TWO_SUBDIR, config);
                final Path elementTypePath =
                        config.containsKey(Config.Keys.DIRECTORY_TO_TRAVERSE) ?
                                Path.of(config.getString(Config.Keys.DIRECTORY_TO_TRAVERSE)) :
                                phase.equals(Runtime.PHASE.ONE) ?
                                        basePath.resolve(phaseOnePath) : basePath.resolve(phaseTwoPath);
                RecursiveDirectoryTraversal.iterator = pathIterator(RuntimeUtil.getCurrentPhase(config), elementTypePath);
                initialized.set(true);
            }
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (RecursiveDirectoryTraversal.class) {
            if (initialized.compareAndSet(true, false)) {
                iterator = null;
            }
        }
    }


    @Override
    public Optional<WorkChunk> getNext() {
        synchronized (iterator) {
            final Optional<Iterator<Path>> oi = Optional.ofNullable(iterator);
            if (!initialized.get() || oi.isEmpty()) {
                throw new IllegalStateException("WorkChunkDriver not initialized");
            }
            try {
                if (!iterator.hasNext()) {
                    return Optional.empty();
                }
            } catch (IllegalStateException ise) {
                throw new RuntimeException(ise);
            }
            final WorkChunk file = EmittableWorkChunkFile.from(iterator.next(), phase, config);
            onNextValue(file);
            return Optional.of(file);
        }
    }

    private static Iterator<Path> pathIterator(final Runtime.PHASE phase, final Path elementTypePath) {
        if (phase.equals(Runtime.PHASE.ONE) || phase.equals(Runtime.PHASE.TWO)) {
            try {
                final List<Path> l = Files.walk(elementTypePath)
                        .filter(file -> !Files.isDirectory(file))
                        .collect(Collectors.toList());
                return l.iterator();
            } catch (IOException e) {
                throw RuntimeUtil
                        .getErrorHandler(RecursiveDirectoryTraversal.class)
                        .handleFatalError(e, phase, elementTypePath);
            }
        }
        throw new IllegalStateException("Unknown phase " + phase);
    }
}
