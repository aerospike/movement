/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 */

package com.aerospike.movement.util.tinkerpop;

import com.aerospike.movement.util.core.stream.sequence.SequenceUtil;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Args {


    public static Map<String, Object> asMap(final Object... args) {
        return asMap(Arrays.asList(args));
    }

    public static Map<String, Object> asMap(final List<Object> args) {
        if (args.size() == 0 || args.size() % 2 != 0) {
            throw new RuntimeException("key value object arrays must have an even, nonzero number of elements");
        }
        final Map<String, Object> results = new HashMap<>();
        final Iterator<Object> iter = args.iterator();
        try {
            while (iter.hasNext()) {
                results.put((String) iter.next(), iter.next());
            }
        } catch (final ClassCastException classCastException) {
            throw new RuntimeException("key value object arrays must have String typed keys");
        }
        return results;
    }


}
