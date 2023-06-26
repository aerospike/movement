package com.aerospike.graph.move.emitter.fileLoader;

import com.aerospike.graph.move.emitter.*;
import com.aerospike.graph.move.encoding.format.csv.GraphCSV;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.structure.EmittedId;
import com.aerospike.graph.move.structure.EmittedIdImpl;
import com.aerospike.graph.move.config.ConfigurationBase;
import com.aerospike.graph.move.util.ErrorUtil;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class GraphCSVImporter implements Emitter {

    private final Configuration config;

    public static class Config extends ConfigurationBase {
        @Override
        public Map<String, String> getDefaults() {
            return DEFAULTS;
        }

        public static class Keys {
            public static final String VERTEX_FILE_PATH = "emitter.rootVertexIdStart";
            public static final String EDGE_FILE_PATH = "emitter.rootVertexIdEnd";
        }

        public static final Map<String, String> DEFAULTS = new HashMap<>();
    }

    public static final GraphCSVImporter.Config CONFIG = new GraphCSVImporter.Config();


    private final String vertexPath;
    private final String edgePath;

    public GraphCSVImporter(Configuration config) {
        this.config = config;
        this.vertexPath = CONFIG.getOrDefault(config, Config.Keys.VERTEX_FILE_PATH);
        this.edgePath = CONFIG.getOrDefault(config, Config.Keys.EDGE_FILE_PATH);
    }

    @Override
    public Stream<Emitable> phaseOneStream() {
        try (Stream<Path> paths = Files.walk(Path.of(vertexPath))) {
            return paths
                    .filter(Files::isRegularFile)
                    .flatMap(path -> EmitableFile.from(path,
                            Runtime.PHASE.ONE,
                            labelFromPath(path),
                            config).stream());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String labelFromPath(final Path path) {
        return path.getParent().getFileName().toString();
    }

    @Override
    public Stream<Emitable> phaseOneStream(final long startId, final long endId) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Stream<Emitable> phaseTwoStream() {
        try (Stream<Path> paths = Files.walk(Path.of(edgePath))) {
            return paths.filter(Files::isRegularFile)
                    .flatMap(path -> {
                        try {
                            final String header = Files.lines(path).findFirst().get();
                            return Files.lines(path)
                                    .filter(line -> !line.startsWith("~"))
                                    .map(line -> new GraphCSV.CSVLine(line, header))
                                    .map(line -> (EmittedEdge) new CSVEdge(line));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<Emitable> phaseTwoStream(long startId, long endId) {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Iterator<Object> phaseOneIterator() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Iterator<Object> phaseTwoIterator() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public Emitter withIdSupplier(Iterator<Object> idSupplier) {
        throw ErrorUtil.unimplemented();
    }


    @Override
    public void close() {

    }

    @Override
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

    @Override
    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return readHeaderFromFileByType(edgePath, label).stream()
                .filter(x -> !x.startsWith("~"))
                .collect(Collectors.toList());
    }


    public class CSVVertex implements EmittedVertex {

        private final GraphCSV.CSVLine line;

        public CSVVertex(GraphCSV.CSVLine line) {
            this.line = line;
        }

        @Override
        public Stream<String> propertyNames() {
            return line.propertyNames().stream();
        }

        @Override
        public Optional<Object> propertyValue(final String name) {
            return Optional.of(line.getEntry(name));
        }

        @Override
        public String label() {
            return (String) line.getEntry("~label");
        }

        @Override
        public Stream<Emitable> emit(final Output writer) {
            writer.vertexWriter(label()).writeVertex(this);
            return Stream.empty();
        }


        @Override
        public Stream<Emitable> stream() {
            return Stream.empty();
        }

        @Override
        public EmittedId id() {
            return new EmittedIdImpl(Long.valueOf((String) line.getEntry("~id")));
        }
    }

    public class CSVEdge implements EmittedEdge {

        private final GraphCSV.CSVLine line;

        public CSVEdge(GraphCSV.CSVLine line) {
            this.line = line;
        }

        @Override
        public EmittedId fromId() {
            return new EmittedIdImpl(Long.valueOf((String) line.getEntry("~from")));
        }

        @Override
        public EmittedId toId() {
            return new EmittedIdImpl(Long.valueOf((String) line.getEntry("~to")));
        }

        @Override
        public Stream<String> propertyNames() {
            return line.propertyNames().stream();
        }

        @Override
        public Optional<Object> propertyValue(final String name) {
            return Optional.of(line.getEntry(name));
        }

        @Override
        public String label() {
            return line.getEntry("~label").toString();
        }

        @Override
        public Stream<Emitable> emit(final Output writer) {
            return Stream.empty();
        }

        @Override
        public Stream<Emitable> stream() {
            return Stream.empty();
        }
    }

}
