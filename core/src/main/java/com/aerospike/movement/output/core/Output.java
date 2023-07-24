package com.aerospike.movement.output.core;

import com.aerospike.movement.runtime.core.Runtime;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Output extends AutoCloseable {

    static void init(Runtime.PHASE phase, Configuration config) {
        RuntimeUtil.closeAllInstancesOfLoadable(Output.class);
    }

    //for graph metadata is label of type string
    OutputWriter writer(Class type, Object metadata);

    Map<String, Object> getMetrics();

    void close();

    void dropStorage();

}
