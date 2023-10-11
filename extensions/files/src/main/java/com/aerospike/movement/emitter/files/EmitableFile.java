package com.aerospike.movement.emitter.files;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.encoding.core.Decoder;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.RuntimeUtil;
import com.aerospike.movement.util.core.iterator.IteratorUtils;
import org.apache.commons.configuration2.Configuration;

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


    protected EmitableFile(final Path path, final Runtime.PHASE phase, String label, final Configuration config) {
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
