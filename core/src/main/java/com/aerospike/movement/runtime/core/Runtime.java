/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core;

import com.aerospike.movement.process.core.Task;
import com.aerospike.movement.runtime.core.local.RunningPhase;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.Future;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Runtime {


    RunningPhase runPhase(PHASE phase,  Configuration config);

    Iterator<RunningPhase> runPhases(List<PHASE> phases, Configuration config);


    Iterator<?> runTask(Task task);

    void close();

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
        @Override
        public String toString(){
            return String.valueOf(value);
        }
    }

}
