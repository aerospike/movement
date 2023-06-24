package com.aerospike.graph.move.emitter.loader;

import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.emitter.Emitter;
import com.aerospike.graph.move.encoding.format.csv.CSVEncoder;
import com.aerospike.graph.move.util.ConfigurationBase;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;


public class CSVImporter implements Emitter {

    public static final CSVImporter.Config CONFIG = new CSVImporter.Config();
    private final String vertexPath;
    private final String edgePath;

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

    public CSVImporter(Configuration config) {
        this.vertexPath = CONFIG.getOrDefault(config, Config.Keys.VERTEX_FILE_PATH);
        this.edgePath = CONFIG.getOrDefault(config, Config.Keys.EDGE_FILE_PATH);
    }

    @Override
    public Stream<EmittedVertex> phaseOneStream() {
        try (Stream<Path> paths = Files.walk(Path.of(vertexPath))) {
            return paths.filter(Files::isRegularFile)
                    .flatMap(path -> {
                        try {
                            final String header = Files.lines(path).findFirst().get();
                            return Files.lines(path)
                                    .filter(line -> !line.startsWith("~"))
                                    .map(line -> new CSVEncoder.CSVLine(line, header))
                                    .map(line -> (EmittedVertex) new CSVVertex(line));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<EmittedVertex> phaseOneStream(final long startId, final long endId) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public Stream<EmittedEdge> phaseTwoStream() {
        try (Stream<Path> paths = Files.walk(Path.of(edgePath))) {
            return paths.filter(Files::isRegularFile)
                    .flatMap(path -> {
                        try {
                            final String header = Files.lines(path).findFirst().get();
                            return Files.lines(path)
                                    .filter(line -> !line.startsWith("~"))
                                    .map(line -> new CSVEncoder.CSVLine(line, header))
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
    public Emitter withIdSupplier(final Iterator<Long> idSupplier) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public List<String> getAllPropertyKeysForVertexLabel(final String label) {
        return getHeader();
    }

    @Override
    public List<String> getAllPropertyKeysForEdgeLabel(final String label) {
        return null;
    }




    public class CSVVertex implements EmittedVertex {

        private final CSVEncoder.CSVLine line;

        public CSVVertex(CSVEncoder.CSVLine line) {
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
            return new GeneratedVertex.GeneratedVertexId(Long.valueOf((String) line.getEntry("~id")));
        }
    }

    public class CSVEdge implements EmittedEdge {

        private final CSVEncoder.CSVLine line;

        public CSVEdge(CSVEncoder.CSVLine line) {
            this.line = line;
        }

        @Override
        public EmittedId fromId() {
            return new GeneratedVertex.GeneratedVertexId(Long.valueOf((String) line.getEntry("~from")));
        }

        @Override
        public EmittedId toId() {
            return new GeneratedVertex.GeneratedVertexId(Long.valueOf((String) line.getEntry("~to")));
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
