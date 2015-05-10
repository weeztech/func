package com.weeztech.db.engine.impl;

import com.weeztech.db.engine.ValueType;
import com.weeztech.utils.Unsf;

/**
 * Created by gaojingxin on 15/4/8.
 */
abstract class MemTool {
    protected final static int MAX_VALUE_SIZE = 0x10000;
    protected final static int MAX_KEY_SIZE = 1024;//1K
    protected final static int KEY_SIZE_MASK = MAX_KEY_SIZE - 1;
    protected final static int CHUNK_HEAD_SIZE = 7;
    protected final static int LEVELS_OFFSET = 3;
    protected final static int MAX_FIELDS = 0x1000;
    protected final static int SUMS_KIND_ADD = 0x8000;
    protected final static int SUMS_KIND_PUT = 0x4000;
    protected final static int SUMS_KIND_DEL = SUMS_KIND_ADD | SUMS_KIND_PUT;
    protected final static int SUMS_KIND_MASK = SUMS_KIND_ADD | SUMS_KIND_PUT;

    protected final static int VALUE_TYPE_NULL = 0;
    protected final static int VALUE_TYPE_NULL2 = 1;
    protected final static int VALUE_TYPE_NULL3 = 2;
    protected final static int VALUE_TYPE_NULL4 = 3;
    protected final static int VALUE_TYPE_NULL5 = 4;
    protected final static int VALUE_TYPE_NULL_MAX = VALUE_TYPE_NULL5;
    protected final static int VALUE_TYPE_0 = 5;
    protected final static int VALUE_TYPE_1 = 6;
    protected final static int VALUE_TYPE_BYTE = 7;
    protected final static int VALUE_TYPE_SHORT = 8;
    protected final static int VALUE_TYPE_INT = 9;
    protected final static int VALUE_TYPE_INT_48 = 10;
    protected final static int VALUE_TYPE_LONG = 11;
    protected final static int VALUE_TYPE_SMALL_CHARS = 12;
    protected final static int VALUE_TYPE_LARGE_CHARS = 13;
    protected final static int VALUE_TYPE_SMALL_TUPLE = 14;
    protected final static int VALUE_TYPE_TUPLE = 15;

    private final static ValueType[] types = new ValueType[16];

    static {
        types[VALUE_TYPE_NULL] = ValueType.NULL;
        types[VALUE_TYPE_NULL2] = ValueType.NULL;
        types[VALUE_TYPE_NULL3] = ValueType.NULL;
        types[VALUE_TYPE_NULL4] = ValueType.NULL;
        types[VALUE_TYPE_NULL5] = ValueType.NULL;
        types[VALUE_TYPE_0] = ValueType.ZERO;
        types[VALUE_TYPE_1] = ValueType.ONE;
        types[VALUE_TYPE_BYTE] = ValueType.BYTE;
        types[VALUE_TYPE_SHORT] = ValueType.SHORT;
        types[VALUE_TYPE_INT] = ValueType.INT;
        types[VALUE_TYPE_INT_48] = ValueType.INT48;
        types[VALUE_TYPE_LONG] = ValueType.LONG;
        types[VALUE_TYPE_SMALL_CHARS] = ValueType.STRING;
        types[VALUE_TYPE_LARGE_CHARS] = ValueType.STRING;
        types[VALUE_TYPE_SMALL_TUPLE] = ValueType.TUPLE;
        types[VALUE_TYPE_TUPLE] = ValueType.TUPLE;
    }

    public static ValueType valueType(int internal) {
        return types[internal];
    }


    protected final static int ENTRY_TYPE_MASK = 0XF;
    protected final static int ENTRY_TYPE_VALUES = 0b0000;
    protected final static int ENTRY_TYPE_SUMS = 0b0001;
    protected final static int ENTRY_TYPE_SMALL = 0b0010;

    protected final static int SUM_TYPE_0 = 5;
    protected final static int SUM_TYPE_BYTE = 11;
    protected final static int SUM_TYPE_SHORT = 12;
    protected final static int SUM_TYPE_INT = 13;
    protected final static int SUM_TYPE_INT_48 = 14;
    protected final static int SUM_TYPE_LONG = 15;

    protected final static int MIN_TINY_SUM = -SUM_TYPE_0;
    protected final static int MAX_TIME_SUM = SUM_TYPE_BYTE - SUM_TYPE_0 - 1;

    protected final static int OFFSET_MASK = 0xFFFFFF;
    protected final static int INVALID_OFFSET = 0;

    public final static long MIN_INT_48 = ValueType.MIN_INT_48;
    public final static long MAX_INT_48 = ValueType.MAX_INT_48;

    protected static int getOffset(long ptr) {
        return Unsf.theUnsafe.getInt(ptr) & OFFSET_MASK;
    }

    protected static void putOffset(long ptr, int offset) {
        Unsf.theUnsafe.putShort(ptr, (short) offset);
        Unsf.theUnsafe.putByte(ptr + 2, (byte) (offset >>> 16));
    }

    protected static int getValueChunkNext(long ptr) {
        return getOffset(ptr + 4);
    }

    protected static int getValueChunkSize(long ptr) {
        return Unsf.theUnsafe.getShort(ptr) & 0xFFFF;
    }

    protected static int getValueChunkUsed(long ptr) {
        return Unsf.theUnsafe.getShort(ptr + 2) & 0xFFFF;
    }

    protected static int getBiggerOffset(long levels, int level) {
        return getOffset(levels + level * 3 + 3);
    }

    protected static int getSmallerOffset(long levels) {
        return getOffset(levels);
    }


    protected static void putBiggerOffset(long levels, int level, int offset) {
        putOffset(levels + level * 3 + 3, offset);
    }

    protected static void putSmallerOffset(long levels, int offset) {
        putOffset(levels, offset);
    }

    protected static void putKeyLevel(long entryPtr, int level) {
        Unsf.theUnsafe.putByte(entryPtr, (byte) level);
    }

    protected static byte getKeyLevel(long entryPtr) {
        return Unsf.theUnsafe.getByte(entryPtr);
    }

    protected static int getKeyOffset(long entryPtr) {
        return getKeyOffsetByLevel(getKeyLevel(entryPtr));
    }

    protected static int getKeyOffsetByLevel(int level) {
        return (LEVELS_OFFSET + (2 * 3)) + level * 3;
    }

    protected static int compareKey(long key1, int keyLen1, long key2, int keyLen2) {
        final int ml = Math.min(keyLen1, keyLen2);
        final int shift;
        int i = 0;
        if (ml > 7) {
            int h = ml - 7;
            do {
                final long a = Long.reverseBytes(Unsf.theUnsafe.getLong(key1 + i) << 8);
                final long b = Long.reverseBytes(Unsf.theUnsafe.getLong(key2 + i) << 8);
                if (a > b) {
                    return 1;
                } else if (a < b) {
                    return -1;
                }
                i += 7;
            } while (i <= h);
            if (i == ml) {
                return keyLen1 - keyLen2;
            }
            shift = ((i - h + 1) << 3);
        } else {
            shift = ((8 - ml) << 3);
        }
        final long a = Long.reverseBytes(Unsf.theUnsafe.getLong(key1 + i) << shift);
        final long b = Long.reverseBytes(Unsf.theUnsafe.getLong(key2 + i) << shift);
        if (a > b) {
            return 1;
        } else if (a < b) {
            return -1;
        }
        return keyLen1 - keyLen2;
    }

    protected static void putKeyLen(long entryPtr, int size, int sumsKind) {
        entryPtr++;
        Unsf.theUnsafe.putShort(entryPtr, (short) (size | sumsKind));
    }

    protected static int getSumsKind(long entryPtr) {
        return Unsf.theUnsafe.getShort(entryPtr + 1) & SUMS_KIND_MASK;
    }

    protected static void setSumsKind(long entryPtr, int sumsKind) {
        entryPtr++;
        Unsf.theUnsafe.putShort(entryPtr, (short) (Unsf.theUnsafe.getShort(entryPtr) & KEY_SIZE_MASK | sumsKind));
    }

    protected static int sumsChunkSize(int sumFieldCount) {
        return (sumFieldCount <= 0x100 ? 1 : 2) + sumFieldCount * 8 + (sumFieldCount + 1) / 2;
    }

    protected static int getKeyLen(long entryPtr) {
        return Unsf.theUnsafe.getShort(entryPtr + 1) & KEY_SIZE_MASK;
    }
}
