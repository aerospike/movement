package com.aerospike.movement.util.core;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.logging.core.Logger;
import com.aerospike.movement.logging.core.LoggerFactory;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LoggingErrorHandler implements ErrorHandler {

    private final Logger logger;
    private final Optional<Object> context;

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
        }

        private static final Map<String, String> DEFAULTS = new HashMap<>() {{
        }};
    }

    public static Config CONFIG = new Config();
    private final Configuration config;
    final Optional<Handler<Throwable>> delegate;

    private LoggingErrorHandler(final Handler<Throwable> delegate, final Object context, final Configuration config) {
        this.config = config;
        this.delegate = Optional.ofNullable(delegate);
        this.context = Optional.ofNullable(context);
        this.logger = LoggerFactory.withContext(context);
    }

    public static ErrorHandler create(final Object context, final Configuration config) {
        return new LoggingErrorHandler(null, context, config);
    }

    public ErrorHandler withTrigger(final Handler<Throwable> handler) {
        return new LoggingErrorHandler(handler, this.context.orElse(null), config);
    }


    @Override
    public RuntimeException handleError(final Throwable t, final Object... context) {
        final Object[] compositeContext = ArrayBuilder.create(context)
                .addToFront(t)
                .addToBack(this.context.orElse(new Object[0]))
                .build();
        logger.error(t.getMessage(), compositeContext);
        delegate.ifPresent(throwableHandler -> {
            throwableHandler.handle(t, compositeContext);
        });
        return new RuntimeException(t);
    }

    @Override
    public RuntimeException handleFatalError(final Throwable e, final Object... context) {
        e.printStackTrace();
        System.exit(1);
        //@todo: this is unreachable code,
        // however it feels cleaner to match the fn signature of the other error handler method
        return new RuntimeException(e);
    }
}
