package com.aerospike.graph.move.emitter.fileLoader;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.encoding.Decoder;
import com.aerospike.graph.move.encoding.format.csv.GraphCSV;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class EmitableFile implements Emitable {
    private final Path path;
    private final Runtime.PHASE phase;
    private final Decoder<String> decoder;
    private final String label;
    private AtomicBoolean emitted = new AtomicBoolean(false);


    private EmitableFile(final Path path, final Runtime.PHASE phase, String label, final Configuration config) {
        this.path = path;
        this.phase = phase;
        this.label = label;
        this.decoder = RuntimeUtil.loadDecoder(config);
    }

    public static EmitableFile from(final Path path,
                                    final Runtime.PHASE phase,
                                    final String label,
                                    final Configuration config) {
        return new EmitableFile(path, phase, label, config);
    }

    @Override
    public Stream<Emitable> emit(Output output) {
        if (!emitted.getAndSet(true)) {
            if (phase.equals(Runtime.PHASE.ONE)) {
                output.vertexWriter(label).writeVertex(this);
            } else {
                output.edgeWriter(label).writeEdge(this);
            }
        }
        output.vertexWriter(label).writeVertex(this);
        return stream();
    }

    @Override
    public Stream<Emitable> stream() {
        try {
            return Files.lines(path)
                    .filter(line -> !decoder.skipEntry(line))
                    .map(decoder::decodeElement);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
