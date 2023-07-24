package com.aerospike.movement.performance.plugin;

import org.apache.commons.configuration2.Configuration;

import java.util.Optional;

public interface LifecyclePlugin {
    Optional<Throwable> setupStage(final String stageName, final Configuration config);

    Optional<Throwable> endStage(final String stageName, final Configuration config);

}
