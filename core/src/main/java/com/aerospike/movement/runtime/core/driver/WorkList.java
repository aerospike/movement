/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.driver;

import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;
import com.aerospike.movement.util.core.iterator.ext.IteratorUtils;
import com.aerospike.movement.util.core.stream.sequence.PotentialSequence;
import com.aerospike.movement.util.core.stream.sequence.SequenceUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.*;
import java.util.stream.Collectors;

public class WorkList implements WorkChunk {

    private final PotentialSequence<WorkItem> pse;
    private final UUID id;

    private WorkList(final List<WorkItem> list) {
        this.id = UUID.randomUUID();
        this.pse = SequenceUtil.fuse(OneShotIteratorSupplier.of(() -> list.iterator()));
    }

    public static WorkList from(final Collection<Object> list) {
        final List<WorkItem> itemList = list.stream().map(it -> WorkItem.class.isAssignableFrom(it.getClass()) ? (WorkItem) it : new WorkItem(it)).collect(Collectors.toList());
        return new WorkList(itemList);
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public Optional<WorkItem> getNext() {
        return (Optional<WorkItem>) pse.getNext();
    }
}
