/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.error;

import com.aerospike.movement.util.core.Builder;
import com.aerospike.movement.runtime.core.Handler;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public interface ErrorHandler {
    public enum ErrorType {
        PARSING
    }

    public static class Keys {
        public static String FATAL_ERROR_HANDLER = "errorHandler.fatal.handler";
        public static String TRIGGER = "errorHandler.trigger";
        public static String CONTEXT = "errorHandler.context";
    }

    AtomicReference<Handler<Throwable>> trigger = new AtomicReference<>(null);

    RuntimeException handleError(Throwable e, Object... context);

    RuntimeException handleFatalError(Throwable e, Object... context);

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

    abstract class ErrorHandlerBuilder implements Builder<ErrorHandler> {

        private final Configuration config;
        private final Class errorHandlerClass;
        private final UUID id;

        public ErrorHandlerBuilder(final Class<? extends ErrorHandler> impl, final Configuration config) {
            this.id = UUID.randomUUID();
            this.config = config;
            this.errorHandlerClass = impl;
        }

        @Override
        public ErrorHandler build() {
            com.aerospike.movement.util.core.Builder.Storage.remove(id);
            return (ErrorHandler) RuntimeUtil.openClass(errorHandlerClass, config);
        }

        @Override
        public ErrorHandlerBuilder memoize(String key) {
            //append the original key to the config
            //append the class that implements open(Configuration) for getting th stored key
            //  Configuration when this is called should contain a special key that passes the Builder Id
            config.setProperty(key, id.toString());
            config.setProperty(Builder.postfixKey(key), id.toString());
            return this;
        }

        public ErrorHandlerBuilder withTrigger(final Handler<Throwable> handler) {
            com.aerospike.movement.util.core.Builder.Storage.set(id, Keys.TRIGGER, handler);
            return memoize(Keys.TRIGGER);

        }

        public ErrorHandlerBuilder withContext(Object context) {
            com.aerospike.movement.util.core.Builder.Storage.set(id, Keys.CONTEXT, context);
            return memoize(Keys.CONTEXT);
        }

        public ErrorHandlerBuilder withFatalErrorHandler(Handler<Throwable> fatalErrorHandler) {
            com.aerospike.movement.util.core.Builder.Storage.set(id, Keys.FATAL_ERROR_HANDLER, fatalErrorHandler);
            return memoize(Keys.FATAL_ERROR_HANDLER);
        }
    }
}
