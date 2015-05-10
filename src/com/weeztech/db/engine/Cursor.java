package com.weeztech.db.engine;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Created by gaojingxin on 15/3/22.
 */
public interface Cursor<T> extends AutoCloseable {
    boolean hasNext();

    T next();

    int remain();

    default void forEachRemaining(Consumer<? super T> action) {
        Objects.requireNonNull(action);
        while (hasNext())
            action.accept(next());
    }

    void close();
}
