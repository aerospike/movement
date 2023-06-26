package com.aerospike.graph.move.process.operations;


import com.aerospike.graph.move.process.Job;
import org.apache.commons.configuration2.Configuration;
import com.aerospike.graph.move.util.ErrorUtil;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Generate extends Job {

    private Generate(Configuration config) {
        super(config);
    }

    public static Generate open(Configuration config) {
        return new Generate(config);
    }


    @Override
    public Map<String, Object> getMetrics() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean succeeded() {
        return false;
    }

    @Override
    public boolean failed() {
        return false;
    }

//    public static Generate
}
