package com.aerospike.graph.move.runtime;

import com.aerospike.graph.move.process.Job;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import org.apache.commons.configuration2.Configuration;

import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Runtime {

    static Runtime getLocalRuntime(final Configuration config) {
        return LocalParallelStreamRuntime.getInstance(config);
    }

    static Optional<Runtime> getGlobalRuntime() {
        return Optional.empty();
    }

    void initialPhase();

    void completionPhase();

    Optional<String> submitJob(Job job);
}
