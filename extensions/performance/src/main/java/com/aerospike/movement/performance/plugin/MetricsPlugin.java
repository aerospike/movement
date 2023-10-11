package com.aerospike.movement.performance.plugin;

import java.util.Map;

public interface MetricsPlugin {
    Map<String, Object> getMetrics();
}
