package com.aerospike.movement.emitter.core;

import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.runtime.core.driver.WorkChunkDriver;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Emitter {

    static void init(Runtime.PHASE phase, Configuration config) {
        RuntimeUtil.closeAllInstancesOfLoadable(Emitter.class);
    }

    Stream<Emitable> stream(final WorkChunkDriver workChunkDriver, final Runtime.PHASE phase);

    //@todo remove from interface, use static class lookups across runtime
    List<String> getAllPropertyKeysForVertexLabel(final String label);

    List<String> getAllPropertyKeysForEdgeLabel(final String label);

    List<Runtime.PHASE> phases();


}
