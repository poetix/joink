package com.codepoetics.juxta;

import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface Streamable<T> {
    static <T> Streamable<T> of(Stream<T> source) {
        return of(source.collect(Collectors.toCollection(LinkedList::new)));
    }

    static <T> Streamable<T> of(Iterable<T> source) {
        return () -> StreamSupport.stream(source.spliterator(), false);
    }

    Stream<T> stream();
}
