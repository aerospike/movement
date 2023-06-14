package com.aerospike.graph.generator.runtime;

import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Runtime {
    void processVertexStream();
    void processEdgeStream();

}
