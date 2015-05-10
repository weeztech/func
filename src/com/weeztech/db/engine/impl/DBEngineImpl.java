package com.weeztech.db.engine.impl;

import com.weeztech.db.engine.*;
import com.weeztech.utils.Unsf;
import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * Created by gaojingxin on 15/3/25.
 */
public final class DBEngineImpl implements DBEngine {
    private final RocksDB db;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions defaultCFOptions;
    private final ThreadPoolExecutor readThreadPool;
    private final ThreadPoolExecutor writeThreadPool;

    public final void shutdown() {
        if (writeThreadPool != null) {
            writeThreadPool.shutdown();
        } else if (readThreadPool != null) {
            readThreadPool.shutdown();
        }
    }

    public final boolean isShutdown() {
        return writeThreadPool.isShutdown();
    }

    public final boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        timeout = unit.toNanos(timeout);
        final long start = System.nanoTime();
        return writeThreadPool.awaitTermination(timeout, TimeUnit.NANOSECONDS) &&
                readThreadPool.awaitTermination(timeout - (System.nanoTime() - start), TimeUnit.NANOSECONDS);
    }

    private void checkShutdown() {
        if (writeThreadPool.isShutdown()) {
            throw new RejectedExecutionException("db shutdown");
        }
    }

    static abstract class DBTaskFuture implements Runnable, DBFuture {
        private Throwable error;
        private volatile boolean isDone;

        protected abstract boolean doRun() throws Throwable;

        @Override
        public void run() {
            try {
                if (!doRun()) {
                    return;
                }
            } catch (Throwable e) {
                done(e);
                throw Unsf.throwException(e);
            }
            done(null);
        }

        final void done(Throwable e) {
            synchronized (this) {
                error = e;
                isDone = true;
                this.notifyAll();
            }
        }

        @Override
        public boolean isDone() {
            return isDone;
        }

        public void await() throws Throwable {
            synchronized (this) {
                for (; ; ) {
                    if (isDone) {
                        if (error != null) {
                            throw error;
                        }
                        return;
                    }
                    this.wait();
                }
            }
        }
    }

    public final DBFuture runRead(DBReadTask task) {
        checkShutdown();
        Objects.requireNonNull(task);
        final DBTaskFuture f = new DBTaskFuture() {
            @Override
            protected boolean doRun() throws Throwable {
                ((DBReadThread) Thread.currentThread()).runTask(task, db.getSnapshot());
                return true;
            }
        };
        readThreadPool.execute(f);
        return f;
    }

    final class DBWriteTaskFuture extends DBTaskFuture {
        private DBWriteTask task;
        final DBReadTask after;
        private Snapshot ss;

        DBWriteTaskFuture(DBWriteTask task, DBReadTask after) {
            this.task = Objects.requireNonNull(task);
            this.after = after;
        }

        @Override
        protected boolean doRun() throws Throwable {
            if (task != null) {
                ((DBWriteThread) Thread.currentThread()).runTask(task);
                task = null;
                if (after != null) {
                    ss = db.getSnapshot(); //Objects.requireNonNull(db.getSnapshot());
                    try {
                        readThreadPool.execute(this);
                        return false;
                    } catch (Throwable e) {
                        db.releaseSnapshot(ss);
                        ss = null;
                        throw e;
                    }
                }
            } else {
                ((DBReadThread) Thread.currentThread()).runTask(after, ss);
            }
            return true;
        }
    }

    public final DBFuture runWrite(DBWriteTask task, DBReadTask after) {
        checkShutdown();
        final DBTaskFuture f = new DBWriteTaskFuture(task, after);
        writeThreadPool.execute(f);
        return f;
    }

    static final byte[] defaultCFName = "default".getBytes();
    private List<ColumnFamilyHandle> cfhs;

    private RocksDB openDB(DBEngineConfig config) throws Throwable {
        final String path = config.getPath();
        final File dbPathFile = new File(path);
        dbPathFile.mkdirs();
        if (!dbPathFile.isDirectory()) {
            throw new IllegalArgumentException("无效文件路径: " + path);
        }
        final Options ops = new Options();
        final List<byte[]> names;
        try {
            names = RocksDB.listColumnFamilies(new Options(), path);
        } catch (Throwable e) {
            throw e;
        } finally {
            ops.dispose();
        }
        final List<ColumnFamilyDescriptor> cfds;
        if (names == null) {
            cfds = new ArrayList<>(1);
        } else {
            cfds = new ArrayList<>(names.size() + 1);
            names.stream().filter(n -> !Arrays.equals(n, defaultCFName)).map(ColumnFamilyDescriptor::new).forEachOrdered(cfds::add);
        }
        cfds.add(new ColumnFamilyDescriptor(defaultCFName, defaultCFOptions));
        final List<ColumnFamilyHandle> cfhs = new ArrayList<>(cfds.size());
        RocksDB aDB = null;
        try {
            aDB = RocksDB.open(dbOptions, path, cfds, cfhs);
            for (int i = cfds.size() - 1; i >= 0; i--) {
                final ColumnFamilyDescriptor cfd = cfds.get(i);
                if (cfd.columnFamilyName()[0] == '_') {
                    final ColumnFamilyHandle cfh = cfhs.get(i);
                    aDB.dropColumnFamily(cfh);
                    cfh.dispose();
                }
            }
        } catch (Throwable e) {
            for (ColumnFamilyHandle h : cfhs) {
                h.dispose();
            }
            if (aDB != null) {
                aDB.dispose();
            }
            throw e;
        }
        this.cfhs = cfhs;
        return aDB;
    }

    static class DBWriteTaskBQ<T> extends ArrayBlockingQueue<T> {
        public DBWriteTaskBQ(int capacity) {
            super(capacity);
        }

        @Override
        public boolean offer(T o) {
            try {
                super.put(o);
                return true;
            } catch (InterruptedException e) {
                throw Unsf.throwException(e);
            }
        }
    }

    static class DBReadTaskBQ<T> extends LinkedBlockingDeque<T> {
        public DBReadTaskBQ(int capacity) {
            super(capacity);
        }

        @Override
        public boolean offer(T o) {
            try {
                if (o instanceof DBWriteTaskFuture) {
                    super.putFirst(o);
                } else {
                    super.putLast(o);
                }
                return true;
            } catch (InterruptedException e) {
                throw Unsf.throwException(e);
            }
        }
    }

    private void configDBOptions() throws Throwable {
        final int mtSize = 64 * 1024 * 1024;
        RocksEnv.getDefault().setBackgroundThreads(Runtime.getRuntime().availableProcessors());
        dbOptions.setMaxBackgroundCompactions(Runtime.getRuntime().availableProcessors());
        dbOptions.setCreateIfMissing(true);
        dbOptions.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
        dbOptions.setMaxOpenFiles(-1);
        dbOptions.setAllowOsBuffer(false);
        defaultCFOptions.setWriteBufferSize(mtSize);
        final BlockBasedTableConfig tfc = new BlockBasedTableConfig();
        tfc.setBlockCacheSize(mtSize * 16);
        tfc.setCacheNumShardBits(4);
        tfc.setCacheIndexAndFilterBlocks(true);
        tfc.setFilter(new BloomFilter(10, false));
        defaultCFOptions.setTableFormatConfig(tfc);
        defaultCFOptions.setTargetFileSizeBase(mtSize);
        defaultCFOptions.setLevelZeroFileNumCompactionTrigger(8);
        defaultCFOptions.setLevelZeroSlowdownWritesTrigger(16);
        defaultCFOptions.setLevelZeroStopWritesTrigger(24);
        defaultCFOptions.setMaxBytesForLevelBase(mtSize * 8);
        defaultCFOptions.setMaxBytesForLevelMultiplier(8);
        defaultCFOptions.setNumLevels(10);
        defaultCFOptions.setMaxSuccessiveMerges(4);
        //defaultCFOptions.useFixedLengthPrefixExtractor(2);
//        final HashSkipListMemTableConfig hsmc = new HashSkipListMemTableConfig();
//        hsmc.setBucketCount(mtSize / 64 / 1024);
//        hsmc.setHeight(20);
//        defaultCFOptions.setMemTableConfig(hsmc);
//        defaultCFOptions.setMemtablePrefixBloomBits(mtSize / 64);
        defaultCFOptions.setMergeOperator(Weez.weezSumOperator);
        //defaultCFOptions.setMemtablePrefixBloomProbes(6);
        //defaultCFOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        //dbOptions.setTableCacheNumshardbits()
    }

    public DBEngineImpl(DBEngineConfig config) throws Throwable {
        dbOptions = new DBOptions();
        defaultCFOptions = new ColumnFamilyOptions();
        configDBOptions();
        db = openDB(config);
        try {
            readThreadPool = new ThreadPoolExecutor(1, config.getMaxReadThreads(), 5, TimeUnit.SECONDS,
                    new DBReadTaskBQ<>(config.getReadTaskQueueCapacity()), DBReadThread::new) {
                protected void terminated() {
                    super.terminated();
                    if (cfhs != null) {
                        for (ColumnFamilyHandle h : cfhs) {
                            h.dispose();
                        }
                    }
                    db.close();
                    dbOptions.dispose();
                }
            };
            writeThreadPool = new ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS,
                    new DBWriteTaskBQ<>(config.getWriteTaskQueueCapacity()), DBWriteThread::new) {
                protected void terminated() {
                    super.terminated();
                    readThreadPool.shutdown();
                }
            };
        } catch (Throwable e) {
            db.close();
            this.shutdown();
            throw e;
        }
    }

    private abstract class DBThread extends Thread {

        final MemTable memTable = new MemTable(db, !(this instanceof DBWriteThread));

        public DBThread(Runnable target) {
            super(target);
        }

        @Override
        public void run() {
            try {
                super.run();
            } finally {
                memTable.dispose();
            }
        }
    }

    private class DBReadThread extends DBThread {

        DBReadThread(Runnable run) {
            super(run);
        }

        final void runTask(DBReadTask task, Snapshot snapshot) throws Throwable {
            try {
                memTable.setSnapshot(snapshot);
                task.run(memTable);
            } finally {
                memTable.reset();
                db.releaseSnapshot(snapshot);
            }
        }
    }

    public static DBWriter dbWriter() {
        return ((DBWriteThread) Thread.currentThread()).memTable;
    }

    public static DBReader dbReader() {
        return ((DBThread) Thread.currentThread()).memTable;
    }

//    public static <R> R get(Function<DBReader, R> call) {
//        return call.apply(((DBThread) Thread.currentThread()).memTable);
//    }

    private class DBWriteThread extends DBThread {

        DBWriteThread(Runnable run) {
            super(run);
        }

        final void runTask(DBWriteTask task) {
            try {
                task.run(memTable);
                memTable.commit();
            } catch (Throwable e) {
                Unsf.theUnsafe.throwException(e);
            } finally {
                memTable.reset();
            }
        }
    }
}
