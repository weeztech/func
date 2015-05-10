package com.weeztech.db.schema.impl;

import com.weeztech.db.engine.DBEngine;
import com.weeztech.db.engine.DBReadTask;
import com.weeztech.db.engine.DBReader;
import com.weeztech.db.schema.DBSchema;
import com.weeztech.db.schema.DBSchemaConfig;
import com.weeztech.db.schema.Table;
import com.weeztech.utils.Unsf;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by gaojingxin on 15/4/18.
 */
public class DBSchemaImpl implements DBSchema {
    @Override
    public void shutdown() {
        dbEngine.shutdown();
    }

    @Override
    public final boolean isShutdown() {
        return dbEngine.isShutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return dbEngine.awaitTermination(timeout, unit);
    }


    private AbstractTable[] tables = EMPTY_TABLES;
    private final HashMap<String, AbstractTable> tablesByName = new HashMap<>();
    static final AbstractTable[] EMPTY_TABLES = new AbstractTable[0];

    private final DBEngine dbEngine;

    private void init(DBReader r) {

    }

    DBSchemaImpl(DBSchemaConfig config) throws Throwable{
        dbEngine = DBEngine.newInstance(config);
        try {
            dbEngine.runRead(this::init).await();
        } catch (Throwable e) {
            shutdown();
            throw e;
        }
    }


    @Override
    public int tableCount() {
        return tables.length;
    }

    @Override
    public Table getTable(int index) {
        return tables[index];
    }

    @Override
    public Table findTable(String name) {
        synchronized (tablesByName) {
            return tablesByName.get(name);
        }
    }
}
