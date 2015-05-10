package com.weeztech.db.engine;

/**
 * Created by gaojingxin on 15/4/2.
 */
@FunctionalInterface
public interface DBReadTask {
    void run(DBReader dbReader)throws Throwable;
}
