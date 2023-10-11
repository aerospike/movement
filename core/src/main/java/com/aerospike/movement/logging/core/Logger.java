package com.aerospike.movement.logging.core;

public interface Logger {
    public void info(String message, Object... context);

    public void error(String message, Object... context);

    public void debug(String message, Object... context);

    public void warn(String message, Object... context);
}
