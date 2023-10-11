package com.aerospike.movement.util.core;

import com.aerospike.movement.emitter.core.graph.EmittedEdge;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public interface ErrorHandler {
    static AtomicReference<Handler<Throwable>> trigger = new AtomicReference<>(null);

    RuntimeException handleError(Throwable e, Object... context);
    RuntimeException handleFatalError(Throwable e, Object... context);

    ErrorHandler withTrigger(Handler<Throwable> handler);

    default Optional<Object> catchError(Callable<?> lambda) {
        try {
            return Optional.ofNullable(lambda.call());
        } catch (Throwable e) {
            handleError(e);
        }
        return Optional.empty();
    }

    default RuntimeException error(String s, Object... context) {
        return handleError(new RuntimeException(String.format(s, context)), context);
    }
}
