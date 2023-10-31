/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.math;

public class ProbUtil {
    public static double compositeProbability(final double x, final double y, final double probScale) {
        return x * y * probScale;
    }

    public static boolean coinFlip(final double weight) {
        return Math.random() < weight;
    }
}
