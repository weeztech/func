package com.weeztech.db.engine;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Created by gaojingxin on 15/4/2.
 */
public interface DBReader {

    DBReader lookup(short category);

    DBReader key(boolean key);

    DBReader key(byte key);

    DBReader key(short key);

    DBReader key(int key);

    DBReader key(long key);

    DBReader key(CharSequence key);

    DBReader key(String key);

    default DBReader key(Object key) {
        if (key instanceof Boolean) {
            key((boolean) key);
        } else if (key instanceof Byte) {
            key((byte) key);
        } else if (key instanceof Short) {
            key((short) key);
        } else if (key instanceof Integer) {
            key((int) key);
        } else if (key instanceof Long) {
            key((long) key);
        } else if (key instanceof String) {
            key((String) key);
        } else if (key instanceof CharSequence) {
            key((CharSequence) key);
        } else {
            throw new IllegalArgumentException("Unsupported key type");
        }
        return this;
    }

    default DBReader keys(Object[] keys) {
        if (keys == null || keys.length == 0) {
            throw new NullPointerException("key");
        }
        for (Object k : keys) {
            key(k);
        }
        return this;
    }

    boolean exist();

    default boolean exist(short category, Object[] keys) {
        return lookup(category).keys(keys).exist();
    }

    <T> T get(KVDecoder<T> decoder);

    default <T> T get(short category, Object[] keys, KVDecoder<T> decoder) {
        return lookup(category).keys(keys).get(decoder);
    }

    DBReader from(short category, boolean exclude);

    DBReader to(boolean exclude);

    default DBReader fromExclude(short category) {
        return from(category, true);
    }

    default DBReader fromInclude(short category) {
        return from(category, false);
    }

    default DBReader toExclude() {
        return to(true);
    }

    default DBReader toInclude() {
        return to(false);
    }

    <T> Cursor<T> cursor(boolean backward, KVDecoder<T> decoder);

    default <T> Cursor<T> forward(KVDecoder<T> decoder) {
        return cursor(false, decoder);
    }

    default <T> Cursor<T> backward(KVDecoder<T> decoder) {
        return cursor(true, decoder);
    }
}
