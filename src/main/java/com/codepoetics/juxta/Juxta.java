package com.codepoetics.juxta;

import java.util.function.Function;
import java.util.stream.Stream;

public interface Juxta<L> {

    static <L> Juxta<L> join(Stream<L> left) {
        return join(Streamable.of(left));
    }

    static <L> Juxta<L> join(Iterable<L> left) {
        return join(Streamable.of(left));
    }

    static <L> Juxta<L> join(Streamable<L> left) {
        return () -> left;
    }

    Streamable<L> left();

    interface JoinBuilder<L, K> {
        <R> Join<L, R, K> to(Function<R, K> primaryKey);
    }

    default <K> JoinBuilder<L, K> on(Function<L, K> foreignKey) {
        return new JoinBuilder<L, K>() {
            @Override
            public <R> Join<L, R, K> to(Function<R, K> primaryKey) {
                return new Join<L, R, K>() {
                    @Override public Streamable<L> left() { return Juxta.this.left(); }
                    @Override public Function<L, K> foreignKey() { return foreignKey; }
                    @Override public Function<R, K> primaryKey() { return primaryKey; }
                };
            }
        };
    }

}
