package com.aerospike.movement.emitter.files;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.runtime.core.driver.WorkList;
import com.aerospike.movement.runtime.core.driver.impl.SuppliedWorkChunkDriver;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class RecursiveDirectoryTraversal extends WorkChunkDriver {
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

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};

    }

    private static AtomicBoolean initialized = new AtomicBoolean(false);
    private static AtomicReference<RecursiveDirectoryTraversal> INSTANCE = new AtomicReference<>();
    private final String phaseOnePath;
    private final String phaseTwoPath;
    private final Iterator<Path> iterator;

    private RecursiveDirectoryTraversal(final Configuration config) {
        super(Config.INSTANCE, config);
        this.phaseOnePath = DirectoryLoader.CONFIG.getOrDefault(DirectoryLoader.Config.Keys.PHASE_ONE_DIRECTORY, config);
        this.phaseTwoPath = DirectoryLoader.CONFIG.getOrDefault(DirectoryLoader.Config.Keys.PHASE_TWO_DIRECTORY, config);
        this.iterator = pathIterator(RuntimeUtil.getCurrentPhase(config));
    }

    @Override
    protected AtomicBoolean getInitialized() {
        return initialized;
    }

    public RecursiveDirectoryTraversal open(final Configuration config) {
        if (initialized.compareAndSet(false, true)) {

            INSTANCE.set(new RecursiveDirectoryTraversal(config));
        }
        return INSTANCE.get();
    }

    @Override
    public void init(final Configuration config) {

    }

    @Override
    public void close() throws Exception {

    }
    @Override
    public Optional<WorkChunk> getNext() {
        synchronized (RecursiveDirectoryTraversal.class) {
            if (!initialized.get()) {
                throw new IllegalStateException("WorkChunkDriver not initialized");
            }
            if (!iterator.hasNext()) {
                return Optional.empty();
            }
            final WorkChunk file = WorkFile.fromPath(iterator.next());
            onNextValue(file);
            return Optional.of(file);
        }
    }

    private Path getPathForPhase(Runtime.PHASE phase) {
        return phase.equals(Runtime.PHASE.ONE) ? Path.of(phaseOnePath) : Path.of(phaseTwoPath);
    }

    private Iterator<Path> pathIterator(final Runtime.PHASE phase) {
        final Path elementTypePath = phase.equals(Runtime.PHASE.ONE) ? Path.of(phaseOnePath) : Path.of(phaseTwoPath);
        if (phase.equals(Runtime.PHASE.ONE) || phase.equals(Runtime.PHASE.TWO)) {
            try (final Stream<Path> walk = Files.walk(elementTypePath)) {
                return walk.iterator();
            } catch (IOException e) {
                errorHandler.handleError(e, this);
            }
        }
        throw new IllegalStateException("Unknown phase " + phase);
    }
}
