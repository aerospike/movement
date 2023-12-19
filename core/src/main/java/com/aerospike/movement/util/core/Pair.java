package com.aerospike.movement.util.core;

public class Pair<A, B> {
    public final Object left;
    public final Object right;

    public Pair(A left, B right) {
        this.left = left;
        this.right = right;
    }

    public static <A, B> Pair<A, B> of(A left, B right) {
        return new Pair<>(left, right);
    }
}
