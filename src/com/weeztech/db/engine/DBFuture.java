package com.weeztech.db.engine;

/**
 * Created by gaojingxin on 15/4/18.
 */
public interface DBFuture {
    boolean isDone();
    void await() throws Throwable;
}
