package com.aerospike.movement.output.core;

import com.aerospike.movement.emitter.core.Emitable;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface OutputWriter {

    //Write an element to the output. Format must be compatible with T
    void writeToOutput(Emitable edge);
    //Initialize output. Write header, etc.
    void init();

    //Flush buffer to output, if any
    void flush();

    //Close output.
    void close();

}
