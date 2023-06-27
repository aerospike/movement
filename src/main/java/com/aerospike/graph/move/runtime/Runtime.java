package com.aerospike.graph.move.runtime;

import com.aerospike.graph.move.process.Job;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import org.apache.commons.configuration2.Configuration;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Runtime {

    static Runtime getLocalRuntime(final Configuration config) {
        return LocalParallelStreamRuntime.getInstance(config);
    }


    void initialPhase();
    void initialPhase(Iterator<List<Object>> iterator);

    void completionPhase();
    void completionPhase(Iterator<List<Object>> iterator);


    Optional<String> submitJob(Job job);

    enum PHASE {
        ONE(1),
        TWO(2);

        private final int value;

        PHASE(int i) {
            this.value = i;
        }

        public int value() {
            return value;
        }

        public static PHASE fromValue(int i) {
            for (PHASE p : PHASE.values()) {
                if (p.value == i) {
                    return p;
                }
            }
            throw new IllegalArgumentException("No phase with value: " + i);
        }
    }
}
