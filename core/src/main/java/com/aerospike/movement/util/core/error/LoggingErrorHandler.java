/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.util.core.error;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.logging.core.Logger;
import com.aerospike.movement.logging.core.LoggerFactory;
import com.aerospike.movement.util.core.ArrayBuilder;
import com.aerospike.movement.util.core.configuration.ConfigurationUtil;
import com.aerospike.movement.runtime.core.Handler;
import org.apache.commons.configuration2.Configuration;

import java.util.*;

public class LoggingErrorHandler implements ErrorHandler {

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
            return ConfigurationUtil.getKeysFromClass(Config.Keys.class);
        }

        public static class Keys {
            public static final String FATAL_ERROR_HANDLER = "FATAL_ERROR_HANDLER";
            public static final String CONTEXT = "CONTEXT";
            public static final String DELEGATE = "DELEGATE";
            public static final String LOGGER = "LOGGER";
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{

        }};
    }


    public static Config CONFIG = new Config();
    private final Configuration config;
    final Optional<Handler<Throwable>> errorDelegate;
    final Handler<Throwable> fatalErrorDelegate;

    private final Optional<Object> context;
    private final Logger logger;

    private LoggingErrorHandler(final Logger logger,
                                final Configuration config,
                                final Optional<Handler<Throwable>> delegate,
                                final Optional<Handler<Throwable>> fatalErrorDelegate,
                                final Optional<Object> context) {
        this.config = config;
        this.errorDelegate = delegate;
        this.fatalErrorDelegate = fatalErrorDelegate.isPresent() ? fatalErrorDelegate.get() : FatalErrorShutdown.open(config);
        this.context = context;
        this.logger = logger;
    }

    public static ErrorHandler open(final Configuration config) {
        final Optional<Handler<Throwable>> delegateOption = Config.INSTANCE.getBuilderOption(Config.Keys.DELEGATE, config);
        final Optional<Object> contextOption = Config.INSTANCE.getBuilderOption(Config.Keys.CONTEXT, config);
        final Optional<Handler<Throwable>> fatalErrorHandlerOption = Config.INSTANCE.getBuilderOption(Config.Keys.FATAL_ERROR_HANDLER, config);
        final Logger logger = LoggerFactory.withContext(contextOption.orElse("none"));
        return new LoggingErrorHandler(logger, config, delegateOption, fatalErrorHandlerOption, contextOption);
    }

    public static LoggingErrorHandlerBuilder builder(Configuration config) {
        return LoggingErrorHandlerBuilder.open(config);
    }

    public static class LoggingErrorHandlerBuilder extends ErrorHandlerBuilder {
        public LoggingErrorHandlerBuilder(final Configuration config) {
            super(LoggingErrorHandler.class, config);
        }

        public static LoggingErrorHandlerBuilder open(final Configuration config) {
            return new LoggingErrorHandlerBuilder(config);
        }
    }


    @Override
    public RuntimeException handleError(final Throwable t, final Object... context) {
        final Object[] compositeContext = ArrayBuilder.create(context)
                .addToFront(t)
                .addToBack(this.context.orElse(new Object[0]))
                .build();
        logger.error(Optional.ofNullable(t.getMessage()).orElse("No Message"), compositeContext);
        t.printStackTrace();
        errorDelegate.ifPresent(throwableHandler -> {
            throwableHandler.handle(t, compositeContext);
        });
        return new RuntimeException(t);
    }

    @Override
    public RuntimeException handleFatalError(final Throwable e, final Object... context) {
        this.printContext(context);
        final RuntimeException exc = handleError(e, context);
        fatalErrorDelegate.handle(e, context);

        //potentially unreachable code, fatalErrorDelegate may call System.exit()
        return exc;
    }

    private void printContext(final Object[] context) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Fatal error encountered, context: ");
        Arrays.asList(context).forEach(it -> {
            sb.append("\n\t");
            sb.append(it.toString());
        });
        System.err.println(sb);
    }
}
