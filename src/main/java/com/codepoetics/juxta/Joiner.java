package com.codepoetics.juxta;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

public class Joiner<L, R, K extends Comparable<K>> {

    private final Index<K, L> leftIndex;
    private final JoinKey<R, K> primaryKey;

    public Joiner(Index<K, L> leftIndex, JoinKey<R, K> primaryKey) {
        this.leftIndex = leftIndex;
        this.primaryKey = primaryKey;
    }

    public Stream<Tuple2<L, R>> manyToOne(Stream<R> rights) {
        return leftIndex.manyToOne(primaryKey.index(rights));
    }

    public Stream<Tuple2<L, R>> manyToOne(Fetcher<K, R> fetcher) {
        Collection<? extends R> rights = fetcher.fetch(leftIndex.keys());
        return leftIndex.manyToOne(primaryKey.index(rights.parallelStream()));
    }

    public Stream<Tuple2<L, Set<R>>> oneToMany(Stream<R> rights) {
        return leftIndex.oneToMany(primaryKey.index(rights));
    }

    public Stream<Tuple2<L, Set<R>>> oneToMany(Fetcher<K, R> fetcher) {
        Collection<? extends R> rights = fetcher.fetch(leftIndex.keys());
        return leftIndex.oneToMany(primaryKey.index(rights.parallelStream()));
    }
}
