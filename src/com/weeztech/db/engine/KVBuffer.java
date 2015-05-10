package com.weeztech.db.engine;

/**
 * Created by gaojingxin on 15/4/8.
 */
public interface KVBuffer {
    boolean booleanKey();

    default int compareKey(boolean key) {
        if (booleanKey()) {
            if (key) {
                return 0;
            } else {
                return -1;
            }
        } else if (key) {
            return 1;
        } else {
            return 0;
        }
    }

    byte byteKey();

    default int compareKey(byte key) {
        return byteKey() - key;
    }

    short shortKey();

    default int compareKey(short key) {
        return shortKey() - key;
    }


    int intKey();

    default int compareKey(int key) {
        return intKey() - key;
    }

    long longKey();

    default int compareKey(long key) {
        return Long.compare(longKey(), key);
    }

    String stringKey();

    int compareKey(String key);

    int compareKey(CharSequence key);

    int stringKey(StringBuilder to);

    boolean keyEOF();

    default int compareKey(Object key) {
        if (key instanceof Boolean) {
            return compareKey((boolean) key);
        } else if (key instanceof Byte) {
            return compareKey((byte) key);
        } else if (key instanceof Short) {
            return compareKey((short) key);
        } else if (key instanceof Integer) {
            return compareKey((int) key);
        } else if (key instanceof Long) {
            return compareKey((long) key);
        } else if (key instanceof String) {
            return compareKey((String) key);
        } else if (key instanceof CharSequence) {
            return compareKey((CharSequence) key);
        } else {
            throw new IllegalArgumentException("Unsupported key type");
        }
    }

    default int compareKeys(Object[] keys) {
        for (Object k : keys) {
            if (keyEOF()) {
                return -1;
            }
            final int c = compareKey(k);
            if (c != 0) {
                return c;
            }
        }
        return keyEOF() ? 0 : 1;
    }

    int values();

    boolean valueWasNull();

    boolean booleanValue();

    default boolean valueEQ(boolean value) {
        final ValueType vt = valueType();
        return (vt == ValueType.ZERO || vt == ValueType.ONE) && booleanValue() == value;
    }

    byte byteValue();

    default boolean valueEQ(byte value) {
        final ValueType vt = valueType();
        if (vt == ValueType.NULL || vt.ordinal() > ValueType.BYTE.ordinal()) {
            return false;
        }
        return byteValue() == value;
    }

    short shortValue();

    default boolean valueEQ(short value) {
        final ValueType vt = valueType();
        return !(vt == ValueType.NULL || vt.ordinal() > ValueType.SHORT.ordinal()) && shortValue() == value;
    }

    int intValue();

    default boolean valueEQ(int value) {
        final ValueType vt = valueType();
        return !(vt == ValueType.NULL || vt.ordinal() > ValueType.INT.ordinal()) && intValue() == value;
    }

    long longValue();

    default boolean valueEQ(long value) {
        final ValueType vt = valueType();
        return !(vt == ValueType.NULL || vt.ordinal() > ValueType.LONG.ordinal()) && longValue() == value;
    }

    String stringValue();

    default boolean valueEQ(String value) {
        final ValueType vt = valueType();
        if (value == null || value.isEmpty()) {
            return vt == ValueType.NULL;
        }
        return vt == ValueType.STRING && value.equals(stringValue());
    }

    default boolean valueEQ(CharSequence value) {
        final ValueType vt = valueType();
        if (value == null || value.length() == 0) {
            return vt == ValueType.NULL;
        }
        return vt == ValueType.STRING && stringValue().contentEquals(value);
    }

    int stringValue(StringBuilder to);

    int skipStringValue();

    int tupleValue();

    default boolean valueEQ(Object value) {
        if (value == null) {
            return valueType() == ValueType.NULL && !booleanValue();
        } else if (value instanceof Boolean) {
            return valueEQ((boolean) value);
        } else if (value instanceof Byte) {
            return valueEQ((byte) value);
        } else if (value instanceof Short) {
            return valueEQ((short) value);
        } else if (value instanceof Integer) {
            return valueEQ((int) value);
        } else if (value instanceof Long) {
            return valueEQ((long) value);
        } else if (value instanceof String) {
            return valueEQ((String) value);
        } else if (value instanceof CharSequence) {
            return valueEQ((CharSequence) value);
        } else if (value instanceof Object[]) {
            return valueEQ((Object[]) value);
        } else {
            throw new IllegalArgumentException("Unsupported key type");
        }
    }

    default boolean valueEQ(Object[] tuple) {
        final ValueType vt = valueType();
        if (tuple == null || tuple.length == 0) {
            return vt == ValueType.NULL;
        }
        if (vt != ValueType.TUPLE) {
            return false;
        }
        final int l = tupleValue();
        if (l != tuple.length) {
            return false;
        }
        for (Object o : tuple) {
            if (!valueEQ(o)) {
                return false;
            }
        }
        return true;
    }

    default boolean valuesEQ(Object[] values) {
        final int l = values();
        if (l != values.length) {
            return false;
        }
        for (Object o : values) {
            if (!valueEQ(o)) {
                return false;
            }
        }
        return true;
    }

    ValueType valueType();

    int sums();

    int intSum();

    long longSum();

    default boolean sumsEQ(long[] sums) {
        final int l = sums();
        if (l != sums.length) {
            return false;
        }
        for (long s : sums) {
            if (s != longSum()) {
                return false;
            }
        }
        return true;
    }
}
