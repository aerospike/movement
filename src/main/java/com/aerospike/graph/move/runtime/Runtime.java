package com.aerospike.graph.move.runtime;

import com.aerospike.graph.move.output.Output;
import com.aerospike.graph.move.process.Job;
import com.aerospike.graph.move.runtime.local.LocalParallelStreamRuntime;
import com.aerospike.graph.move.runtime.local.RunningPhase;
import org.apache.commons.configuration2.Configuration;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ForkJoinTask;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Runtime {

    static Runtime getLocalRuntime(final Configuration config) {
        return LocalParallelStreamRuntime.getInstance(config);
    }


    RunningPhase phaseOne();
    Map.Entry<ForkJoinTask, List<Output>> phaseOne(Iterator<List<Object>> iterator);

    RunningPhase phaseTwo();
    RunningPhase phaseTwo(Iterator<List<Object>> iterator);


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
