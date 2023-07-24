package com.aerospike.movement.encoding.core;

import com.aerospike.movement.emitter.core.Emitable;
import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Map;
import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Encoder<O> {
    class Keys {
        public static final String METADATA = "metadata";
    }

    static void init(Runtime.PHASE phase, Configuration config) {
        RuntimeUtil.closeAllInstancesOfLoadable(Encoder.class);
    }

    O encode(Emitable item);

    Optional<O> encodeItemMetadata(Emitable item);

    Map<String, Object> getEncoderMetadata();

    void close();
}
