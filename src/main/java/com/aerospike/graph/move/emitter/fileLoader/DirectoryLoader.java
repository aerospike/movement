package com.aerospike.graph.move.emitter.fileLoader;

import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.encoding.Decoder;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DirectoryLoader extends Emitter.PhasedEmitter {

    private final Decoder<String> decoder;

    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String VERTEX_FILE_PATH = "emitter.vertexFilePath";
            public static final String EDGE_FILE_PATH = "emitter.edgeFilePath";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>();
    }

    public static final DirectoryLoader.Config CONFIG = new DirectoryLoader.Config();


    private final String vertexPath;
    private final String edgePath;
    private final Configuration config;


    private DirectoryLoader(Configuration config) {
        this.config = config;
        this.vertexPath = CONFIG.getOrDefault(config, Config.Keys.VERTEX_FILE_PATH);
        this.edgePath = CONFIG.getOrDefault(config, Config.Keys.EDGE_FILE_PATH);
        this.decoder = RuntimeUtil.loadDecoder(config);
    }

    public static DirectoryLoader open(Configuration config) {
        return new DirectoryLoader(config);
    }

    @Override
    public Stream<Emitable> stream(Runtime.PHASE phase) {
        return stream(IteratorUtils.stream(getDriverForPhase(phase)).flatMap(l -> l.stream()).iterator(), phase);
    }

    @Override
    public Stream<Emitable> stream(final Iterator<Object> filePathIterator, final Runtime.PHASE phase) {
        List<Object> a = IteratorUtils.list(filePathIterator);
        return IteratorUtils.stream(a.iterator())
                .map(object -> {
                    final Emitable x = EmitableFile.from(
                            (Path) object,
                            phase,
                            labelFromPath((Path) object),
                            config);
                    return x;
                });
    }

    @Override
    public Iterator<List<Object>> getDriverForPhase(final Runtime.PHASE phase) {
        final Path elementTypePath = phase.equals(Runtime.PHASE.ONE) ? Path.of(vertexPath) : Path.of(edgePath);
        if (phase.equals(Runtime.PHASE.ONE) || phase.equals(Runtime.PHASE.TWO)) {
            Iterator<List<Object>> x = getOrCreateDriverIterator(phase, (ignored) -> {
                try (Stream<Path> paths = Files.walk(elementTypePath)) {
                    final List<Path> l = paths.filter(Files::isRegularFile).collect(Collectors.toList());
                    Iterator<Path> i = l.iterator();
                    assert i.hasNext();
                    return Optional.ofNullable(i)
                            .orElseThrow(() -> new IllegalStateException("Phase one iterator not initialised"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            return x;
        }
        throw new IllegalStateException("Unknown phase " + phase);

    }

    private static String labelFromPath(final Path path) {
        return path.getParent().getFileName().toString();
    }


    @Override
    public void close() {

    }

    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        return readHeaderFromFileByType(vertexPath, label).stream()
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

    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return readHeaderFromFileByType(edgePath, label).stream()
                .filter(x -> !x.startsWith("~"))
                .collect(Collectors.toList());
    }

    @Override
    public List<Runtime.PHASE> phases() {
        return List.of(Runtime.PHASE.ONE, Runtime.PHASE.TWO);
    }
}
