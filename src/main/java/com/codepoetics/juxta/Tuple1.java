package com.codepoetics.juxta;

import java.util.function.BiFunction;

public interface Tuple1<T1, T2> {

    static <T1, T2> Tuple1<T1, T2> of(T1 first, T2 second) {
        return new Tuple1<T1, T2>() {
            @Override public T1 first() { return first; }
            @Override public T2 second() { return second; }
        };
    }
    T1 first();
    T2 second();
    default public <O> O construct(BiFunction<T1, T2, O> constructor) {
        return constructor.apply(first(), second());
    }
}
