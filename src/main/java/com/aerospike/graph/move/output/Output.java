package com.aerospike.graph.move.output;

import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import com.aerospike.graph.move.util.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Output {
    OutputWriter vertexWriter(String label);

    OutputWriter edgeWriter(String label);

    Long getEdgeMetric();

    Long getVertexMetric();

    void close();

    void dropStorage();

    static void init(int phase, Configuration config) {
        if (Boolean.parseBoolean(LocalParallelStreamRuntime.CONFIG.getOrDefault(config, LocalParallelStreamRuntime.Config.Keys.DROP_OUTPUT))) {
            RuntimeUtil.loadOutput(config).dropStorage();
        }
    }
}
