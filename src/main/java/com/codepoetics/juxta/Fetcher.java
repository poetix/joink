package com.codepoetics.juxta;

import java.util.Collection;

public interface Fetcher<K, R> {
    Collection<? extends R> fetch(Collection<? extends K> keys);
}
