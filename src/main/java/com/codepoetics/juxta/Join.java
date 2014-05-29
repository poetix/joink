package com.codepoetics.juxta;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Join<L, R, K> {
    Streamable<L> left();
    Function<L, K> foreignKey();
    Function<R, K> primaryKey();

    default public Set<K> foreignKeys() {
        return left().stream().map(foreignKey()).collect(Collectors.toSet());
    }

    default public Stream<Tuple1<L, R>> manyToOne(Streamable<R> rights) {
        return manyToOne(foreignKeys(), rights);
    }

    default public Stream<Tuple1<L, R>> manyToOne(Set<K> fks, Streamable<R> rights) {
        Function<R, K> pk = primaryKey();
        Function<L, K> fk = foreignKey();

        Map<K, R> lookup = rights.stream()
                .filter(r -> fks.contains(pk.apply(r)))
                .collect(Collectors.toMap(pk, Function.identity()));
        return left().stream().map(l -> Tuple1.of(l, lookup.get(fk.apply(l))));
    }

    default public Stream<Tuple1<L, R>> manyToOne(Function<Iterable<K>, Iterable<R>> fetcher) {
        Set<K> fks = foreignKeys();
        Iterable<R> rights = fetcher.apply(fks);
        return manyToOne(fks, Streamable.of(rights));
    }

    default public Stream<Tuple1<L, List<R>>> manyToMany(Streamable<R> rights) {
        return manyToMany(foreignKeys(), rights);
    }

    default public Stream<Tuple1<L, List<R>>> manyToMany(Function<Iterable<K>, Iterable<R>> fetcher) {
        Set<K> fks = foreignKeys();
        Iterable<R> rights = fetcher.apply(fks);
        return manyToMany(fks, Streamable.of(rights));
    }

    default public Stream<Tuple1<L, List<R>>> manyToMany(Set<K> fks, Streamable<R> rights) {
        Function<R, K> pk = primaryKey();
        Function<L, K> fk = foreignKey();

        Map<K, List<R>> lookup = rights.stream().filter(r -> fks.contains(pk.apply(r)))
                .collect(Collectors.groupingBy(pk));
        return left().stream().map(l -> Tuple1.of(l, lookup.get(fk.apply(l))));
    }

}
