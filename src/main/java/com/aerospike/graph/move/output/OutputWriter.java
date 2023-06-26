package com.aerospike.graph.move.output;

import com.aerospike.graph.move.emitter.Emitable;
import com.aerospike.graph.move.emitter.EmittedEdge;
import com.aerospike.graph.move.emitter.EmittedVertex;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface OutputWriter {

    //Write an element to the output. Format must be compatible with T
    void writeEdge(Emitable edge);
    void writeVertex(Emitable vertex);

    //Initialize output. Write header, etc.
    void init();

    //Flush buffer to output, if any
    void flush();

    //Close output.
    void close();

}
