/*
 * @author Grant Haywood <grant.haywood@aerospike.com>
 * Developed May 2023 - Oct 2023
 * Copyright (c) 2023 Aerospike Inc.
 *
 */

package com.aerospike.movement.runtime.core.local;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.logging.core.LoggerFactory;
import com.aerospike.movement.util.core.error.ErrorHandler;
import com.aerospike.movement.util.core.runtime.RuntimeUtil;
import org.apache.commons.configuration2.Configuration;

import java.util.Optional;
import java.util.UUID;

public abstract class Loadable implements AutoCloseable {
    protected final UUID id;
    private final ConfigurationBase configurationMeta;
    protected final ErrorHandler errorHandler;
    protected final Configuration config;
    public boolean closed = false;

    protected Loadable(final ConfigurationBase configurationMeta, final Configuration config) {
        this.config = config;
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

    public Configuration getConfiguration() {
        return config;
    }


    public ConfigurationBase getConfigurationMeta() {
        return configurationMeta;
    }

    public interface Notification {
        Object getMessage();
    }

    @Override
    public final void close() {
        if (!closed)
            onClose();
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    public abstract void onClose();

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + this.getId().toString().split("-")[0];
    }
}
