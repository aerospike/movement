/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.test.mock;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.logging.core.Logger;
import com.aerospike.movement.logging.core.LoggerFactory;
import com.aerospike.movement.util.core.configuration.ConfigUtil;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.error.LoggingErrorHandler;
import com.aerospike.movement.runtime.core.Handler;
import org.apache.commons.configuration2.Configuration;

import java.util.*;

public class MockErrorHandler implements ErrorHandler {

    public static class Config extends ConfigurationBase {
        public static final Config INSTANCE = new Config();

        private Config() {
            super();
        }

        @Override
        public Map<String, String> defaultConfigMap(final Map<String, Object> config) {
            return DEFAULTS;
        }

        @Override
        public List<String> getKeys() {
            return ConfigUtil.getKeysFromClass(LoggingErrorHandler.Config.Keys.class);
        }

        public static class Keys {

        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }

    public static class Methods {
        public static final String HANDLE_ERROR = "handleError";
        public static final String HANDLE_FATAL_ERROR = "handleFatalError";
    }

    private MockErrorHandler(final Handler<Throwable> delegate, final Object context, final Configuration config) {
        this.config = config;
        this.delegate = Optional.ofNullable(delegate);
        this.context = Optional.ofNullable(context);
        this.logger = LoggerFactory.withContext(context);
    }

    public static ErrorHandler open(final Configuration config) {
        Config.INSTANCE.getOrDefault(ErrorHandler.Keys.CONTEXT, config);
        return new MockErrorHandler(null, null, config);
    }

    public static MockErrorHandlerBuilder builder(final Configuration config) {
        return new MockErrorHandlerBuilder(config);
    }

    public static class MockErrorHandlerBuilder extends ErrorHandlerBuilder {
        public MockErrorHandlerBuilder(Configuration config) {
            super(MockErrorHandler.class, config);
        }

        public static MockErrorHandlerBuilder open(final Configuration config) {
            return new MockErrorHandlerBuilder(config);
        }
    }


    private final Configuration config;
    private final Optional<Handler<Throwable>> delegate;
    private final Optional<Object> context;
    private final Logger logger;


    @Override
    public RuntimeException handleError(final Throwable t, final Object... context) {
        MockUtil.incrementHitCounter(this.getClass(), Methods.HANDLE_ERROR);
        return (RuntimeException) MockUtil
                .onEvent(this.getClass(), Methods.HANDLE_ERROR, this)
                .orElse(LoggingErrorHandler.builder(config).withContext(this).build().handleError(t, context));
    }

    @Override
    public RuntimeException handleFatalError(final Throwable t, final Object... context) {
        MockUtil.incrementHitCounter(this.getClass(), Methods.HANDLE_FATAL_ERROR);
        return (RuntimeException) MockUtil
                .onEvent(this.getClass(), Methods.HANDLE_FATAL_ERROR, this)
                .orElse(LoggingErrorHandler.builder(config).withContext(this).build().handleFatalError(t, context));
    }


}
