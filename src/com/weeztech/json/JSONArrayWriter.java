package com.weeztech.json;

import java.time.Duration;
import java.time.Instant;

/**
 * Created by gaojingxin on 15/3/25.
 */
public interface JSONArrayWriter {
    public void append(int value);

    public void append(long value);

    public void append(double value);

    public void append(boolean value);

    public void append(Instant value);

    public void append(Duration value);

    public void append(String value);

    public <T> void appendObj(T obj, JSONObjBuilder<T> builder);

    public <A> void appendArray(A obj, JSONArrayBuilder<A> builder);
}
