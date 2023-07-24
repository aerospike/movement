package com.aerospike.movement.util.core;

public interface Handler<T> {
    void handle(T e, Object... context);
}
