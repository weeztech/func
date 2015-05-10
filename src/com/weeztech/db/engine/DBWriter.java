package com.weeztech.db.engine;

import java.lang.reflect.Array;

/**
 * Created by gaojingxin on 15/4/2.
 */
public interface DBWriter extends DBReader {
    DBWriter write(short category);

    default DBWriter write(short category, Object[] keys, Object[] values) {
        return write(category).keys(keys).values(values);
    }

    DBWriter key(boolean key);

    DBWriter key(byte key);

    DBWriter key(short key);

    DBWriter key(int key);

    DBWriter key(long key);

    DBWriter key(CharSequence key);

    DBWriter key(String key);

    default DBWriter key(Object key) {
        DBReader.super.key(key);
        return this;
    }

    default DBWriter keys(Object[] keys) {
        DBReader.super.keys(keys);
        return this;
    }

    DBWriter values(int fieldCount);

    default DBWriter values(Object[] values) {
        if (values == null || values.length == 0) {
            throw new NullPointerException("values");
        }
        values(values.length);
        for (Object v : values) {
            value(v);
        }
        return this.end();
    }

    DBWriter valueNull();

    DBWriter value(boolean value);

    DBWriter value(int value);

    DBWriter value(long value);

    DBWriter value(CharSequence value);

    DBWriter value(String value);

    DBWriter value(CharSequence value, int start, int end);

    DBWriter value(String value, int start, int end);

    default DBWriter value(Object value) {
        if (value == null) {
            valueNull();
        } else if (value instanceof Boolean) {
            value((boolean) value);
        } else if (value instanceof Byte) {
            value((byte) value);
        } else if (value instanceof Short) {
            value((short) value);
        } else if (value instanceof Integer) {
            value((int) value);
        } else if (value instanceof Long) {
            value((long) value);
        } else if (value instanceof String) {
            value((String) value);
        } else if (value instanceof CharSequence) {
            value((CharSequence) value);
        } else if (value.getClass().isArray()) {
            int l = Array.getLength(value);
            valueTuple(l);
            for (int i = 0; i < l; i++) {
                value(Array.get(value, i));
            }
        } else {
            throw new IllegalArgumentException("Unsupported key type");
        }
        return this;
    }

    DBWriter valueTuple(int fields);

    DBWriter addSums(int fieldCount);

    default DBWriter addSums(long[] adds) {
        if (adds == null || adds.length == 0) {
            throw new NullPointerException("adds");
        }
        addSums(adds.length);
        for (long a : adds) {
            add(a);
        }
        return end();
    }

    default DBWriter addSums(short category, Object[] keys, long[] adds) {
        return write(category).keys(keys).addSums(adds);
    }


    DBWriter putSums(int fieldCount);

    default DBWriter putSums(long[] adds) {
        if (adds == null || adds.length == 0) {
            throw new NullPointerException("adds");
        }
        putSums(adds.length);
        for (long a : adds) {
            add(a);
        }
        return end();
    }

    default DBWriter putSums(short category, Object[] keys, long[] adds) {
        return write(category).keys(keys).putSums(adds);
    }

    DBWriter add(long value);

    DBWriter end();

    DBWriter delete();

    default DBWriter delete(short category, Object[] keys) {
        return write(category).keys(keys).delete();
    }
}
