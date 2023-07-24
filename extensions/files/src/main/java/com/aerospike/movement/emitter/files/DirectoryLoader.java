package com.aerospike.movement.emitter.files;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.emitter.core.Emitter;
import com.aerospike.movement.process.core.Loadable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorHandler;
import com.aerospike.movement.util.core.Handler;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DirectoryLoader extends Loadable implements Emitter {


    @Override
    public void init(final Configuration config) {

    }


    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String,Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }


        public static class Keys {
            public static final String PHASE_ONE_DIRECTORY = "loader.vertexFilePath";
            public static final String PHASE_TWO_DIRECTORY = "loader.edgeFilePath";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
            put(Keys.PHASE_ONE_DIRECTORY, "verticies");
            put(Keys.PHASE_TWO_DIRECTORY, "edges");
        }};
    }

    public static final Config CONFIG = new Config();
    private final Configuration config;

    private ErrorHandler errorHandler;

    private DirectoryLoader(final Configuration config) {
        super(Config.INSTANCE, config);
        this.config = config;
        this.errorHandler = RuntimeUtil.loadErrorHandler(this, config);
    }

    public static DirectoryLoader open(final Configuration config) {
        return new DirectoryLoader(config);
    }

    @Override
    public Stream<Emitable> stream(WorkChunkDriver driver, final Runtime.PHASE phase) {
        return IteratorUtils.stream(IteratorUtils.map(driver.iterator(), chunk -> {
            WorkFile file = (WorkFile) chunk;
            final Emitable x = EmitableFile.from(
                    file.getPath(),
                    phase,
                    labelFromPath(file.getPath()),
                    config);
            return x;
        }));
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

    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return readHeaderFromFileByType(CONFIG.getOrDefault(Config.Keys.PHASE_ONE_DIRECTORY, config), label).stream()
                .filter(x -> !x.startsWith("~"))
                .collect(Collectors.toList());
    }


    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        return readHeaderFromFileByType(CONFIG.getOrDefault(Config.Keys.PHASE_TWO_DIRECTORY, config), label).stream()
                .filter(x -> !x.startsWith("~"))
                .collect(Collectors.toList());
    }

    private List<String> readHeaderFromFileByType(String basePath, String label) {
        try {
            return Files.walk(Path.of(basePath + "/" + label))
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
