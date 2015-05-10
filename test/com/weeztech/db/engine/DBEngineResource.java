package com.weeztech.db.engine;

import com.weeztech.utils.Unsf;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Created by gaojingxin on 15/4/29.
 */
public final class DBEngineResource extends ExternalResource {
    private final TemporaryFolder dbFolder = new TemporaryFolder();
    private DBEngine db;

    public DBFuture runRead(DBReadTask task) {
        return db.runRead(task);
    }

    public DBFuture runWrite(DBWriteTask task, DBReadTask after) {
        return db.runWrite(task, after);
    }

    public DBFuture runWrite(DBWriteTask task) {
        return runWrite(task, null);
    }

    @Override
    protected void before() throws Throwable {
        DBEngineConfig config = new DBEngineConfig(dbFolder.getRoot().getAbsolutePath());
        db = DBEngine.newInstance(config);
    }

    @Override
    protected void after() {
        if (db != null) {
            try {
                db.close();
                db = null;
            } catch (Throwable e) {
                throw Unsf.throwException(e);
            }
        }
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return dbFolder.apply(super.apply(base, description), description);
    }
}
