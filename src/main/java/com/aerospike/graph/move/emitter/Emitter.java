package com.aerospike.graph.move.emitter;

import com.aerospike.graph.move.runtime.Runtime;
import com.aerospike.graph.move.util.MovementIteratorUtils;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Emitter {
    static void init(int value, Configuration config) {
        phaseOneStarted.set(false);
        phaseTwoStarted.set(false);
    }

    Stream<Emitable> stream(Runtime.PHASE phase);

    Stream<Emitable> stream(Iterator<Object> iterator, Runtime.PHASE phase);

    Iterator<List<Object>> getDriverForPhase(Runtime.PHASE phase);

    List<String> getAllPropertyKeysForVertexLabel(final String label);

    List<String> getAllPropertyKeysForEdgeLabel(final String label);

    List<Runtime.PHASE> phases();

    void close();


    AtomicBoolean phaseOneStarted = new AtomicBoolean(false);
    AtomicBoolean phaseTwoStarted = new AtomicBoolean(false);

    abstract class PhasedEmitter implements Emitter {
        protected static Iterator<List<Object>> phaseOneIterator;
        protected static Iterator<List<Object>> phaseTwoIterator;

        private Iterator<List<Object>> wrapChunkedSyncronized(Iterator<Object> it) {
            return MovementIteratorUtils.Threadsafe.SyncronizedBatchIterator.create(it, 1000);
        }

        protected Iterator<List<Object>> getOrCreateDriverIterator(Runtime.PHASE phase, Function<Void, Iterator<?>> createDriver) {
            if (phase.equals(Runtime.PHASE.ONE) && phases().contains(Runtime.PHASE.ONE)) {
                if (phaseOneStarted.compareAndSet(false, true)) {
                    Iterator<Object> x = (Iterator<Object>) createDriver.apply(null);
                    assert x.hasNext();
                    phaseOneIterator = wrapChunkedSyncronized(x);
                    assert phaseOneIterator.hasNext();
                }
                return Optional.ofNullable(phaseOneIterator).orElseThrow(() ->
                        new RuntimeException("Phase one iterator is null"));
            } else if (phase.equals(Runtime.PHASE.TWO) && phases().contains(Runtime.PHASE.TWO)) {
                if (phaseTwoStarted.compareAndSet(false, true)) {
                    Iterator<Object> x = (Iterator<Object>) createDriver.apply(null);
                    assert x.hasNext();
                    phaseTwoIterator = wrapChunkedSyncronized(x);
                    assert phaseTwoIterator.hasNext();
                }
                return Optional.ofNullable(phaseTwoIterator).orElseThrow(() -> new RuntimeException("Phase two iterator is null"));
            }
            return Collections.emptyIterator();
        }
    }

}
