package com.aerospike.movement.test.mock;

import com.aerospike.movement.logging.core.LoggerFactory;

import java.util.Optional;

public class MockCallback {
    Optional<Object> onEvent(final Object object, final Object... args) {
        LoggerFactory.withContext(object).info("Mock callback: " + argsToString(args));
        return onEvent(object, args);
    }

    public static MockCallback create(final MockHandler handler) {
        return new MockCallback() {
            @Override
            Optional<Object> onEvent(final Object object, final Object... args) {
                return handler.handleEvent(object, args);
            }
        };
    }

    private String argsToString(final Object[] args) {
        final StringBuilder sb = new StringBuilder();
        for (final Object arg : args) {
            sb.append(arg.toString()).append(",");
        }
        return sb.toString();
    }
}
