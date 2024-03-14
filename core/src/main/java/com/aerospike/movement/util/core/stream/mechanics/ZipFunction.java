package com.aerospike.movement.util.core.stream.mechanics;

import java.util.Optional;
import java.util.function.BiFunction;

public interface ZipFunction<A, B, Z> extends BiFunction<A, B, Optional<Z>> {
    ZipFunction<Integer, Integer, Integer> zipAddition = ZipFunction.from((integer, integer2) -> Optional.of(integer + integer2));

    static <A, B, Z> ZipFunction<A, B, Z> from(BiFunction<A, B, Optional<Z>> zipFunction) {
        return zipFunction::apply;
    }

}
