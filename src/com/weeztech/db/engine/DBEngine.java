package com.weeztech.db.engine;

import com.weeztech.AbstractServer;
import com.weeztech.db.engine.impl.DBEngineImpl;

/**
 * Created by gaojingxin on 15/4/7.
 */
public interface DBEngine extends AbstractServer {

    DBFuture runRead(DBReadTask task);

    DBFuture runWrite(DBWriteTask task, DBReadTask after);

    default DBFuture runWrite(DBWriteTask task) {
        return runWrite(task, null);
    }

    static DBEngine newInstance(DBEngineConfig config) throws Throwable{
        return new DBEngineImpl(config);
    }
}