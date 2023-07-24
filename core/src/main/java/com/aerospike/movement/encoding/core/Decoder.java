package com.aerospike.movement.encoding.core;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

public interface Decoder<O>  {
    static void init(Runtime.PHASE phase, Configuration config) {
        RuntimeUtil.closeAllInstancesOfLoadable(Decoder.class);
    }
    Emitable decodeElement(O encodedElement, String label, Runtime.PHASE phase);

    void close();

    boolean skipEntry(O line);
}
