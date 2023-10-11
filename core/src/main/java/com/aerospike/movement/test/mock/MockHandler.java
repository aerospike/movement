package com.aerospike.movement.test.mock;

import java.util.Optional;

public interface MockHandler  {
    Optional<Object> handleEvent(final Object object, final Object... args);

}
