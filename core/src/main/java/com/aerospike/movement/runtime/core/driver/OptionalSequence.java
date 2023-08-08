package com.aerospike.movement.runtime.core.driver;

import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface OptionalSequence<T> {
    Optional<T> getNext();
}
