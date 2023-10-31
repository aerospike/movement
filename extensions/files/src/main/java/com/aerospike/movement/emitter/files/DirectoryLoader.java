/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.emitter.files;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.runtime.core.local.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.movement.emitter.files.RecursiveDirectoryTraversal.Config.Keys.DIRECTORY_TO_TRAVERSE;
import static org.apache.commons.configuration2.ConfigurationUtils.copy;


public class DirectoryLoader extends Loadable implements Emitter, Emitter.SelfDriving, Emitter.Constrained {

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
            public static final String LABEL = "loader.label";
            public static final String BASE_PATH = "loader.basePath";
            public static final String PHASE_ONE_SUBDIR = "loader.vertexDir";
            public static final String PHASE_TWO_SUBDIR = "loader.edgeDir";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

    public static final Config CONFIG = new Config();
    private final Configuration config;

    private ErrorHandler errorHandler;

    private DirectoryLoader(final Configuration config) {
        super(Config.INSTANCE, config);
        this.config = new MapConfiguration(new HashMap<>());
        copy(config, this.config);
        this.errorHandler = RuntimeUtil.getErrorHandler(this, config);
    }

    public static DirectoryLoader open(final Configuration config) {
        return new DirectoryLoader(config);
    }


    public Path getBaseDirectory(final Runtime.PHASE phase) {
        if (config.containsKey(DIRECTORY_TO_TRAVERSE))
            return Path.of(config.getString(DIRECTORY_TO_TRAVERSE));
        return Path.of((String) CONFIG.getOrDefault(Config.Keys.BASE_PATH, config));
    }

    @Override
    public void init(final Configuration config) {

    }


    @Override
    public WorkChunkDriver driver(final Configuration callerConfig) {
        Path driverPath = getBaseDirectory(RuntimeUtil.getCurrentPhase(config));
        for (final String constraint : getConstraints(callerConfig))
            driverPath = driverPath.resolve(constraint);
        final String pathName = driverPath.toString();
        final RecursiveDirectoryTraversal directoryTraversal = RecursiveDirectoryTraversal.open(ConfigurationUtil.configurationWithOverrides(config, new HashMap<>() {{
            put(DIRECTORY_TO_TRAVERSE, pathName);
        }}));
        directoryTraversal.init(config);
        return directoryTraversal;
    }

    @Override
    public List<String> getConstraints(final Configuration callerConfig) {
        final List<String> constraintConfigs = ConfigurationUtil.getSubKeys(callerConfig, Keys.CONSTRAINT);
        final List<String> x = constraintConfigs.stream()
                .sorted((numericStringKey1, numericStringKey2) -> {
                    try {
                        final Integer i1 = Integer.parseInt(ConfigurationUtil.getLastConfigPathElement(numericStringKey1));
                        final Integer i2 = Integer.parseInt(ConfigurationUtil.getLastConfigPathElement(numericStringKey2));
                        return i1.compareTo(i2);
                    } catch (Exception e) {
                        throw RuntimeUtil
                                .getErrorHandler(this)
                                .handleFatalError(e, ErrorHandler.ErrorType.PARSING, callerConfig);
                    }
                })
                .map(it -> {
                    final String value = Optional.ofNullable(callerConfig.getString(it)).orElseThrow();
                    return value;
                })
                .collect(Collectors.toList());
        return x;
    }

    @Override
    public Stream<Emitable> stream(final WorkChunkDriver workChunkDriver, final Runtime.PHASE phase) {
        return Stream.iterate(workChunkDriver.getNext(), Optional::isPresent, i -> workChunkDriver.getNext())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(chunk -> {
                    try {
                        return ((EmittableWorkChunkFile) chunk).stream();
                    } catch (ClassCastException classCastException) {
                        throw new RuntimeException(classCastException);
                    }
                });
    }


    @Override
    public List<Runtime.PHASE> phases() {
        return List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO);
    }

    @Override
    public void close() {

    }

    private static String labelFromPath(final Path path) {
        return path.getParent().getFileName().toString();
    }

    public static Path getPhasePath(final Runtime.PHASE phase, final Configuration config) {
        return Path.of((String)CONFIG.getOrDefault(Config.Keys.BASE_PATH, config))
                .resolve((String) CONFIG.getOrDefault(phase.equals(Runtime.PHASE.ONE) ?
                        Config.Keys.PHASE_ONE_SUBDIR : Config.Keys.PHASE_TWO_SUBDIR, config));
    }


    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return readHeaderFromFileByType(getPhasePath(Runtime.PHASE.TWO, config), label).stream()
                .filter(x -> !x.startsWith("~"))
                .collect(Collectors.toList());
    }


    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        return readHeaderFromFileByType(getPhasePath(Runtime.PHASE.ONE, config), label).stream()
                .filter(x -> !x.startsWith("~"))
                .collect(Collectors.toList());
    }

    private static List<String> readHeaderFromFileByType(final Path typePath, final String label) {
        try {
            return Files.walk(Path.of(typePath + "/" + label))
                    .filter(Files::isRegularFile)
                    .map(path -> {
                        try {
                            return Files.lines(path).findFirst().get();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .findFirst()
                    .map(header -> Arrays.asList(header.split(",")))
                    .orElseThrow(() -> new RuntimeException("No header found for label " + label));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
