package com.aerospike.movement.test.mock;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.test.mock.output.MockOutput;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorHandler;
import com.aerospike.movement.util.core.Handler;
import com.aerospike.movement.util.core.LoggingErrorHandler;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockErrorHandler implements ErrorHandler {

    public final Configuration config = new MapConfiguration(new HashMap<>());

    @Override
    public RuntimeException handleError(final Throwable t, final Object... context) {
        MockUtil.incrementHitCounter(this.getClass(), Methods.HANDLE_ERROR);
        MockUtil.onEvent(this.getClass(), Methods.HANDLE_ERROR, this)
                .orElse(LoggingErrorHandler.create(this, config));
        return new RuntimeException(t);
    }

    @Override
    public RuntimeException handleFatalError(final Throwable t, final Object... context) {
        MockUtil.incrementHitCounter(this.getClass(), Methods.HANDLE_FATAL_ERROR);
        MockUtil.onEvent(this.getClass(), Methods.HANDLE_FATAL_ERROR, this)
                .orElse(LoggingErrorHandler.create(this, config));
        return new RuntimeException(t);
    }

    @Override
    public ErrorHandler withTrigger(final Handler<Throwable> handler) {
        MockUtil.incrementHitCounter(this.getClass(), Methods.WITH_TRIGGER);
        return (ErrorHandler) MockUtil.onEvent(this.getClass(), Methods.WITH_TRIGGER, this, handler)
                .orElse(LoggingErrorHandler.create(this, config).withTrigger(handler));
    }

    public static class Methods {
        public static final String WITH_TRIGGER = "withTrigger";
        public static final String HANDLE_ERROR = "handleError";
        public static final String HANDLE_FATAL_ERROR = "handleFatalError";
    }

}
