package com.aerospike.graph.move.emitter.fileLoader;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.encoding.Decoder;
import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
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

    public static Emitable from(final Path path,
                                final Runtime.PHASE phase,
                                final String label,
                                final Configuration config) {
        return new EmitableFile(path, phase, label, config);
    }

    @Override
    public Stream<Emitable> emit(Output output) {
        return stream().flatMap(ele -> ele.emit(output));
    }

    @Override
    public Stream<Emitable> stream() {
        try {
            Iterator<String> i = Files.lines(path).iterator();
            String header = i.next();

            return IteratorUtils.stream(i)
                    .filter(line -> !decoder.skipEntry(line))
                    .map(it -> {
                        return decoder.decodeElement(it, header, phase);
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
