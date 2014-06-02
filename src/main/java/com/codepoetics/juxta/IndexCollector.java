package com.codepoetics.juxta;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collector;

final class IndexCollector {
    public IndexCollector() { }
    static <K extends Comparable<K>, S> Collector<S, SortedMap<K, Set<S>>, SortedMap<K, Set<S>>> on(Function<? super S, ? extends K> key) {
        return Collector.<S, SortedMap<K, Set<S>>>of(
                TreeMap::new,
                (map, element) -> {
                    K keyValue = key.apply(element);
                    Set<S> elements = map.get(keyValue);
                    if (elements == null) {
                        elements = new HashSet<>();
                        elements.add(element);
                        map.put(keyValue, elements);
                    } else {
                        elements.add(element);
                    }
                },
                (m1, m2) -> {
                    m1.putAll(m2);
                    return m1;
                }
        );
    }
}
