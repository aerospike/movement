package com.aerospike.graph.generator.output;

import com.aerospike.graph.generator.emitter.EmittedEdge;
import com.aerospike.graph.generator.emitter.EmittedVertex;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface OutputWriter {

    //Write an element to the output. Format must be compatible with T
    void writeEdge(EmittedEdge edge);
    void writeVertex(EmittedVertex vertex);

    //Initialize output. Write header, etc.
    void init();

    //Flush buffer to output, if any
    void flush();

    //Close output.
    void close();

}
