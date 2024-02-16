/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver;

import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;
import com.aerospike.movement.util.core.stream.sequence.PotentialSequence;
import com.aerospike.movement.util.core.stream.sequence.SequenceUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;

public class WorkList implements WorkChunk {

    private final PotentialSequence<?> pse;
    private final List<?> list;
    private final Configuration config;
    private final UUID id;

    private WorkList(final List<?> list, final Configuration config) {
        this.config = config;
        this.id = UUID.randomUUID();
        this.list = list;
        this.pse = SequenceUtil.fuse(OneShotIteratorSupplier.of(() -> list.iterator()));
    }

    public static WorkList from(final Collection<Object> list, final Configuration config) {
        return new WorkList(new ArrayList<>(list), config);
    }

//    @Override
//    public WorkItem next() {
//        return new WorkItem(iterator.next());
//    }
//
//    @Override
//    public boolean hasNext() {
//        if (!iterator.hasNext()) {
//            this.onComplete(config);
//            return false;
//        }
//        return true;
//    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public Optional<WorkItem> getNext() {
        return (Optional<WorkItem>) pse.getNext();
    }
}
