package com.weeztech.json;

import java.time.Duration;
import java.time.Instant;

/**
 * Created by gaojingxin on 15/3/25.
 */
public interface JSONObjWriter {
    public void append(String name, int value);

    public void append(String name, long value);

    public void append(String name, double value);

    public void append(String name, boolean value);

    public void append(String name, Instant value);

    public void append(String name, Duration value);

    public void append(String name, String value);

    public <T> void appendObj(String name, T obj, JSONObjBuilder<T> builder);

    public <A> void appendArray(String name, A obj, JSONArrayBuilder<A> builder);
}
