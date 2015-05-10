package com.weeztech;

import java.util.concurrent.TimeUnit;

/**
 * Created by gaojingxin on 15/4/18.
 */
public interface AbstractServer extends AutoCloseable{
    boolean isShutdown();

    void shutdown();

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    default void close() throws InterruptedException {
        shutdown();
        awaitTermination(365, TimeUnit.DAYS);
    }
}
