package com.weeztech.db.engine;

import java.util.Objects;

/**
 * Created by gaojingxin on 15/4/8.
 */
public class DBEngineConfig {
    private final String path;

    public String getPath() {
        return path;
    }

    private int maxReadThreads = Runtime.getRuntime().availableProcessors() * 8;

    public int getMaxReadThreads() {
        return maxReadThreads;
    }

    public void setMaxReadThreads(int maxReadThreads) {
        this.maxReadThreads = Math.max(maxReadThreads, 4);
    }

    private int readTaskQueueCapacity = 1024;

    public int getReadTaskQueueCapacity() {
        return readTaskQueueCapacity;
    }

    public void setReadTaskQueueCapacity(int readTaskQueueCapacity) {
        this.readTaskQueueCapacity = Math.max(readTaskQueueCapacity, 16);
    }

    private int writeTaskQueueCapacity = 1024;

    public int getWriteTaskQueueCapacity() {
        return writeTaskQueueCapacity;
    }

    public void setWriteTaskQueueCapacity(int writeTaskQueueCapacity) {
        this.writeTaskQueueCapacity = Math.max(writeTaskQueueCapacity, 16);
    }

    public DBEngineConfig(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("empty path");
        }
        this.path = path;
    }
}
