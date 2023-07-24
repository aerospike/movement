package com.aerospike.movement.logging.files.impl;

import com.aerospike.movement.encoding.core.Encoder;
import com.aerospike.movement.logging.core.LogMessage;
import com.aerospike.movement.logging.core.Logger;
import com.aerospike.movement.output.core.Output;
import com.aerospike.movement.output.files.DirectoryOutput;
import org.apache.commons.configuration2.Configuration;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SingleSynchronizedFileLogger extends OutputLogger {
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    public static Logger INSTANCE;
    protected SingleSynchronizedFileLogger(final Encoder<String> encoder, final Output output, final Configuration config) {
        super(encoder, output, config);
    }

    public static Logger open(Configuration config) {
        if(initialized.compareAndSet(false, true)) {
            Output output = DirectoryOutput.open(config);
            Encoder<String> encoder = CSVLogMessageEncoder.open(config);
            INSTANCE = new SingleSynchronizedFileLogger(encoder, output, config);
        }
        return INSTANCE;
    }

    @Override
    void log(final LogMessage message) {
        synchronized (SingleSynchronizedFileLogger.class){
            writeToOutput(message);
        }
    }
    @Override
    public void close(){

    }

}

