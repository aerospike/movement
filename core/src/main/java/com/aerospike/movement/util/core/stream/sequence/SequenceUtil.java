package com.aerospike.movement.util.core.stream.sequence;

import com.aerospike.movement.runtime.core.Handler;
import com.aerospike.movement.util.core.error.ErrorUtil;
import com.aerospike.movement.util.core.iterator.OneShotIteratorSupplier;
import com.aerospike.movement.util.core.stream.sequence.PotentialSequence;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class SequenceUtil {
    public static <X> PotentialSequence<X> create(final OneShotIteratorSupplier supplier) {
        throw ErrorUtil.unimplemented();
    }

    public static Optional<?> takeNext(final Object nextable) {
        if (Iterator.class.isAssignableFrom(nextable.getClass())) {
            return ((Iterator<?>) nextable).hasNext() ? Optional.of(((Iterator<?>) nextable).next()) : Optional.empty();
        } else if (PotentialSequence.class.isAssignableFrom(nextable.getClass())) {
            return ((PotentialSequence<?>) nextable).getNext();
        } else {
            throw new RuntimeException(nextable.getClass() + "is not supported by StreamUtil.takeNext");
        }
    }


    public static <SEQ, X, Y> Stream<Map.Entry<Optional<X>, Optional<Y>>> zipUneven(final SEQ a, final SEQ b) {
        final Map.Entry<Optional<?>, Optional<?>> initial = Map.entry(takeNext(a), takeNext(b));
        final Predicate<Map.Entry<Optional<?>, Optional<?>>> hasNext = optionalOptionalEntry ->
                optionalOptionalEntry.getKey().isPresent() || optionalOptionalEntry.getValue().isPresent();
        return Stream.iterate(initial, hasNext, (UnaryOperator) o -> (Object) Map.entry(takeNext(a), takeNext(b)));
    }

    public static <SEQ, X, Y> Stream<Map.Entry<X, Y>> zipMatched(final SEQ a, final SEQ b) {
        return zipUneven(a, b)
                .filter(it -> it.getKey().isPresent() && it.getValue().isPresent())
                .map(it -> Map.entry((X) it.getKey().get(), (Y) it.getValue().get()));
    }

    public static <SEQ, X, Y, Z> Stream<Optional<Z>> join(final SEQ Sa, final SEQ Sb, final BiFunction<Optional<X>, Optional<Y>, Optional<Z>> joiner) {
        return zipUneven(Sa, Sb)
                .map((abPair) ->
                        joiner.apply((Optional<X>) abPair.getKey(), (Optional<Y>) abPair.getValue()));
    }

    public static <A, B, J> Stream<Map.Entry<A, B>> split(final Stream<J> unityStream, final Function<J, Map.Entry<A, B>> splitter) {
        return unityStream.map(splitter);
    }

    public static <Z> Stream<Z> collapse(final Stream<Optional<Z>> sparseStream) {
        return sparseStream.filter(Optional::isPresent).map(Optional::get);
    }

    public static Stream<Map.Entry<String, Object>> unzipEven(final Stream<Object> stream) {
        throw ErrorUtil.unimplemented();
    }

    public static <A, B> Map.Entry<Stream<A>, Stream<B>> unzip(final Stream<Map.Entry<A, B>> zipped) {
        final PotentialSequence<A> leftHand;
        final PotentialSequence<B> rightHand;
        /*
        if you ask for the next on left or right first, an element is materialized from zipped
        if you then ask for the other hand, the other half of that materialized element is returned
        if you ask for the same hand twice, the operation blocks until the other half has been consumed
         */

        throw ErrorUtil.unimplemented();
    }

    public static <T> PotentialSequence<T> fuse(final OneShotIteratorSupplier<T> iteratorSupplier) {
        return fuse(iteratorSupplier, (e, context) -> {
            throw new RuntimeException(e);
        });
    }

    public static <T> PotentialSequence<T> fuse(final OneShotIteratorSupplier<T> iteratorSupplier, final Handler<Throwable> errorHandler) {
        return new PotentialSequence<T>() {
            private final Iterator<T> iterator = iteratorSupplier.get();

            @Override
            public Optional<T> getNext() {
                synchronized (iterator) {
                    try {
                        if (!iterator.hasNext()) {
                            return Optional.empty();
                        } else {
                            return Optional.of(iterator.next());
                        }
                    } catch (final Exception e) {
                        errorHandler.handle(e, this, iterator);
                        return Optional.empty();
                    }
                }
            }
        };
    }

    public static class Entangled<A, B> implements PotentialSequence<Map.Entry<A, B>> {
        CountDownLatch latch;
        @Override
        public Optional<Map.Entry<A, B>> getNext() {
            return Optional.empty();
        }
        //a stream that unzips produces 2 entangled elements
        //stream.unjoin().entangle().unzip()
        //each of the unjoined pairs is wrapped in entangle
        //a countdownlatch embedded in both of them preventss the origin stream from produceing a new element
        //untill both left and right hand have been consumed
        // j -> a b -> E1(a b) -> unzip  -> S(En(a)) S(En(b))
        // in the origin potentialSequence, getNext should block until the last emitted pair are both derefrenced
        // the entangled E1(a b) optional sequence should block until both of the last ones have been emitted

    }
}
/*
unzip the gearbox output to seperated types
a b a b ->
  split ->
    <a,b>,<a,b> ->
      unzip ->
        a 0 a 0 -> collapse -> a a
        0 b 0 b -> collapse -> b b
a b c d a a b a  ->
  split ->
    <a,b>, <c,d> <a, a>, <b,a> ->
      unzip ->
     a b    0 0   a  a   b  a -> collapse -> a b a a b a
       split -> <a,b> <a,a> <b a>
         unzip ->
         a 0 a a 0 a collapse -> a a a a
         0 b 0 0 b 0 collapse -> b b

     0 0    c d   0  0   0  0 -> collapse -> c d
        unzip ->
        c 0 collapse -> c
        0 d collapse -> d

 */
