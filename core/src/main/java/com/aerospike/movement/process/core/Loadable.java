package com.aerospike.movement.process.core;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.logging.core.LoggerFactory;
import com.aerospike.movement.util.core.ConfigurationUtil;
import com.aerospike.movement.util.core.ErrorHandler;
import com.aerospike.movement.util.core.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Optional;
import java.util.UUID;

public abstract class Loadable implements AutoCloseable {
    final UUID id;
    private final ConfigurationBase configurationMeta;
    protected final ErrorHandler errorHandler;

    protected Loadable(final ConfigurationBase configurationMeta, final Configuration config) {
        this.errorHandler = RuntimeUtil.getErrorHandler(this, config);
        this.id = UUID.randomUUID();
        this.configurationMeta = configurationMeta;
    }

    public final UUID getId() {
        return id;
    }

    public Optional<Object> notify(final Notification n) {
        LoggerFactory.withContext(this).debug("Received notify");
        return Optional.empty();
    }

    public abstract void init(Configuration config);

    public ConfigurationBase getConfigurationMeta() {
        return configurationMeta;
    }

    public interface Notification {
        Object getMessage();
    }
}
