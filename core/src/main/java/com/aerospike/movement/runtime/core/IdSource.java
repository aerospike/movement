package com.aerospike.movement.runtime.core;
import java.util.Optional;

public interface IdSource {
    Object getId(Optional<Object> workId);
}
