package com.aerospike.graph.move.encoding;

import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedElement;
import com.aerospike.graph.move.emitter.EmittedVertex;
import com.aerospike.graph.move.runtime.Runtime;
import com.google.common.io.Files;
import org.apache.commons.configuration2.Configuration;

public interface Decoder<O> {


    static void init(final int value, final Configuration config) {
    }

    EmittedElement decodeElement(O encodedEdge);

    O decodeElementMetadata(EmittedElement element);

    O decodeElementMetadata(String label);

    String getExtension();

    void close();


    boolean skipEntry(O line);
}
