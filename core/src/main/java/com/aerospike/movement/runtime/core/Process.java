package com.aerospike.movement.runtime.core;

import com.aerospike.movement.config.core.ConfigurationBase;
import com.aerospike.movement.util.core.RuntimeUtil;

import java.util.List;

public interface Process {

    default List<String> getRequiredConfigKeys() {
        return (List<String>) RuntimeUtil
                .invokeMethod(
                        this.getClass(),
                        "getRequiredConfigKeys",
                        null,
                        new Object[]{})
                .orElseThrow();
    }


    ConfigurationBase getConfig();

}
