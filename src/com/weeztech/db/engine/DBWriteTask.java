package com.weeztech.db.engine;

/**
 * Created by gaojingxin on 15/4/2.
 */
@FunctionalInterface
public interface DBWriteTask {
    public void run(DBWriter dbWriter)throws Throwable;
}
