package com.aerospike.movement.util.core.iterator;

import com.aerospike.movement.runtime.core.driver.OutputIdDriver;

public interface IdDriverFactory {
    OutputIdDriver get();
}
