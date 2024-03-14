package com.aerospike.movement.tinkerpop.common;

public interface Provider<T, C> {
    T getProvided(C ctx);
}
