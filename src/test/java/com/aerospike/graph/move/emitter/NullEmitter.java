package com.aerospike.graph.move.emitter;

import com.aerospike.graph.move.runtime.Runtime;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class NullEmitter implements Emitter{

    public NullEmitter(Configuration config) {

    }

    public static NullEmitter open(Configuration config){
        return new NullEmitter(config);
    }
    @Override
    public Stream<Emitable> stream(Runtime.PHASE phase) {
        return stream(Collections.emptyIterator(), phase);
    }

    @Override
    public Stream<Emitable> stream(Iterator<Object> iterator, Runtime.PHASE phase) {
        return IteratorUtils.stream(iterator).map(o -> (Emitable) o);
    }

    @Override
    public Iterator<List<Object>> getDriverForPhase(Runtime.PHASE phase) {
        return Collections.emptyIterator();
    }

    @Override
    public void close() {

    }

    @Override
    public List<String> getAllPropertyKeysForVertexLabel(String label) {
        return List.of("a");
    }

    @Override
    public List<String> getAllPropertyKeysForEdgeLabel(String label) {
        return List.of("b");
    }
}
