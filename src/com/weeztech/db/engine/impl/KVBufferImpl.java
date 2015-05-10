package com.weeztech.db.engine.impl;

import com.weeztech.db.engine.KVBuffer;
import com.weeztech.db.engine.ValueType;
import com.weeztech.utils.Unsf;

import java.util.Objects;

/**
 * Created by gaojingxin on 15/4/21.
 */
final class KVBufferImpl extends MemTool implements KVBuffer {
    private final static int BEGIN_CTRL_BITS = 0xFF00;
    private final static int END_CTRL_BITS = BEGIN_CTRL_BITS >>> 8;
    private final static int LEFT4_CTRL_BITS_MIN = END_CTRL_BITS << 4;
    private final static int LEFT5_CTRL_BITS_MIN = END_CTRL_BITS << 5;
    private MemTable memTable;
    private boolean valueWasNull;
    private long vPtr;
    private int vLen;
    private int vOffset;
    private int vNext;
    private int valueCtrlBits;

    public KVBufferImpl(MemTable memTable) {
        this.memTable = memTable;
    }

    private void reset(long kPtr, int kLen, long vPtr, int vLen, int vNext) {
        valueWasNull = false;
        valueCtrlBits = END_CTRL_BITS;
        vOffset = 0;
        this.vPtr = vPtr;
        this.vLen = vLen;
        this.vNext = vNext;
        kOffset = 0;
        keyBoolBits = END_BOOL_BITS;
        this.kPtr = kPtr + 2;
        this.kLen = kLen - 2;
    }

    private int nextValueChuck(int need) {
        if (vOffset - need == vLen) valid:{
            while (vNext != 0) {
                final long p = memTable.block + vNext;
                vNext = getValueChunkNext(vPtr);
                vLen = getValueChunkUsed(vPtr);
                if (vLen > need) {
                    vPtr = p + CHUNK_HEAD_SIZE;
                    vOffset = need;
                    return 0;
                } else if (vLen > 0) {
                    break valid;
                }
            }
            throw new IllegalStateException("end of buffer!");
        }
        throw new IllegalStateException("bad get format!");
    }

    private byte rawByteValue() {
        int of = vOffset;
        if (++vOffset > vLen) {
            of = nextValueChuck(1);
        }
        return Unsf.theUnsafe.getByte(vPtr + of);
    }

    private void valueCtrlBits() {
        int of = vOffset;
        if (++vOffset > vLen) {
            of = nextValueChuck(1);
        }
        valueCtrlBits = (Unsf.theUnsafe.getByte(vPtr + of) & 0xFF) | BEGIN_CTRL_BITS;
    }

    private short rawShortValue() {
        int of = vOffset;
        if ((vOffset += 2) > vLen) {
            of = nextValueChuck(2);
        }
        return Unsf.theUnsafe.getShort(vPtr + of);
    }

    private void skipRawShort() {
        if ((vOffset += 2) > vLen) {
            nextValueChuck(2);
        }
    }

    private void skipRawByte() {
        if ((++vOffset) > vLen) {
            nextValueChuck(1);
        }
    }

    private int rawIntValue() {
        int of = vOffset;
        if ((vOffset += 4) > vLen) {
            of = nextValueChuck(4);
        }
        return Unsf.theUnsafe.getInt(vPtr + of);
    }

    private long rawInt48Value() {
        int of = vOffset;
        if ((vOffset += 6) > vLen) {
            of = nextValueChuck(6);
        }
        return Unsf.theUnsafe.getLong(vPtr + of) << 16 >> 16;
    }


    private long rawLongValue() {
        int of = vOffset;
        if ((vOffset += 8) > vLen) {
            of = nextValueChuck(8);
        }
        return Unsf.theUnsafe.getLong(vPtr + of);
    }

    private void addSums(KVBufferImpl b2) {
        final long vPtrSave = vPtr;
        final int vNextSave = vNext;
        final int vLenSave = vLen;
        final int vOffsetSave = vOffset;
        final int l = b2.sums();
        if (sums() != l) {
            throw new IllegalStateException("number of sum fields not match");
        }
        for (int i = 0; i < l; i++) {
            next4CtrlBits();
            int of = vOffset;
            if ((vOffset += 8) > vLen) {
                of = nextValueChuck(8);
            }
            final long p = vPtr + of;
            Unsf.theUnsafe.putLong(p, Unsf.theUnsafe.getLong(p) + b2.longSum());
        }
        valueCtrlBits = END_CTRL_BITS;
        vOffset = vOffsetSave;
        vPtr = vPtrSave;
        vLen = vLenSave;
        vNext = vNextSave;
        memTable.recycleKVBuffer(b2);
    }


    final void addSums(long vPtr2, int vLen2) {
        addSums(memTable.allocKVBuffer().resetFromDB(0, 0, vPtr2, vLen2));
    }

    final void addSums(long vResult) {
        addSums(memTable.allocKVBuffer().resetFromDB(0, 0, vResult));
    }

    private int nextValueCtrlBit() {
        if (valueCtrlBits == END_CTRL_BITS) {
            valueCtrlBits();
        }
        final int bit = valueCtrlBits & 1;
        valueCtrlBits >>>= 1;
        return bit;
    }

    private void endStringCtrlBits() {
        if (valueCtrlBits >= LEFT4_CTRL_BITS_MIN) {
            while (valueCtrlBits >= LEFT5_CTRL_BITS_MIN) {
                valueCtrlBits >>>= 1;
            }
        } else {
            valueCtrlBits = END_CTRL_BITS;
        }
    }

//    final boolean isSumEntry() {
//        return (Unsf.theUnsafe.getByte(vPtr + vOffset) & ENTRY_TYPE_SUMS) != 0;
//    }

    private int nextValueType() {
        if (valueCtrlBits == END_CTRL_BITS) {
            valueCtrlBits();
        }
        int t = valueCtrlBits & 0xF;
        if (VALUE_TYPE_NULL < t && t <= VALUE_TYPE_NULL_MAX) {
            valueCtrlBits = (valueCtrlBits & ~0xF) + t - 1;
            t = VALUE_TYPE_NULL;
        } else {
            valueCtrlBits >>>= 4;
        }
        return t;
    }

    private int next4CtrlBits() {
        if (valueCtrlBits == END_CTRL_BITS) {
            valueCtrlBits();
        }
        int t = valueCtrlBits & 0xF;
        valueCtrlBits >>>= 4;
        return t;
    }

    public final ValueType valueType() {
        if (valueCtrlBits == END_CTRL_BITS) {
            valueCtrlBits();
        }
        return valueType(valueCtrlBits & 0xF);
    }

    @Override
    public final int values() {
        final int t = next4CtrlBits();
        if ((t & ENTRY_TYPE_SUMS) != 0) {
            throw new IllegalStateException("not values");
        }
        int count = next4CtrlBits() + 1;
        if ((t & ENTRY_TYPE_SMALL) == 0) {
            count += (rawByteValue() & 0xFF) << 4;
        }
        return count;
    }

    public final boolean valueWasNull() {
        return valueWasNull;
    }

    public final boolean booleanValue() {
        valueWasNull = false;
        switch (nextValueType()) {
            case VALUE_TYPE_NULL:
                valueWasNull = true;
            case VALUE_TYPE_0:
                return false;
            case VALUE_TYPE_1:
                return true;
        }
        throw new IllegalStateException("not bool get");
    }

    public final byte byteValue() {
        final int t = nextValueType();
        if (valueWasNull = (t == VALUE_TYPE_NULL)) {
            return 0;
        } else if (t < VALUE_TYPE_BYTE) {
            return (byte) (t - VALUE_TYPE_0);
        } else if (t == VALUE_TYPE_BYTE) {
            return rawByteValue();
        } else {
            throw new IllegalStateException("not byte get");
        }
    }

    public final short shortValue() {
        final int t = nextValueType();
        if (valueWasNull = (t == VALUE_TYPE_NULL)) {
            return 0;
        } else if (t < VALUE_TYPE_BYTE) {
            return (short) (t - VALUE_TYPE_0);
        } else if (t == VALUE_TYPE_BYTE) {
            return rawByteValue();
        } else if (t == VALUE_TYPE_SHORT) {
            return rawShortValue();
        } else {
            throw new IllegalStateException("not short get");
        }
    }

    public final int intValue() {
        final int t = nextValueType();
        if (valueWasNull = (t == VALUE_TYPE_NULL)) {
            return 0;
        } else if (t < VALUE_TYPE_BYTE) {
            return t - VALUE_TYPE_0;
        } else if (t == VALUE_TYPE_BYTE) {
            return rawByteValue();
        } else if (t == VALUE_TYPE_SHORT) {
            return rawShortValue();
        } else if (t == VALUE_TYPE_INT) {
            return rawIntValue();
        } else {
            throw new IllegalStateException("not int get");
        }
    }

    public int tupleValue() {
        final int t = nextValueType();
        if (valueWasNull = (t == VALUE_TYPE_NULL)) {
            return 0;
        } else if (t == VALUE_TYPE_SMALL_TUPLE) {
            return next4CtrlBits() + 1;
        } else if (t == VALUE_TYPE_TUPLE) {
            return (next4CtrlBits() | ((rawByteValue() & 0xFF) << 4)) + 1;
        } else {
            throw new IllegalStateException("not tuple get");
        }
    }

    public final long longValue() {
        final int t = nextValueType();
        if (valueWasNull = (t == VALUE_TYPE_NULL)) {
            return 0;
        } else if (t < VALUE_TYPE_BYTE) {
            return t - VALUE_TYPE_0;
        } else if (t == VALUE_TYPE_BYTE) {
            return rawByteValue();
        } else if (t == VALUE_TYPE_SHORT) {
            return rawShortValue();
        } else if (t == VALUE_TYPE_INT) {
            return rawIntValue();
        } else if (t == VALUE_TYPE_INT_48) {
            return rawInt48Value();
        } else if (t == VALUE_TYPE_LONG) {
            return rawLongValue();
        } else {
            throw new IllegalStateException("not long get");
        }
    }

    public final String stringValue() {
        final StringBuilder sb = memTable.ensureSSB();
        sb.setLength(0);
        return stringValue(sb) == 0 ? null : sb.toString();
    }

    public final int skipStringValue() {
        final int t = nextValueType();
        final int l;
        switch (t) {
            case VALUE_TYPE_NULL:
                return 0;
            case VALUE_TYPE_SMALL_CHARS:
                l = next4CtrlBits() + 1;
                break;
            case VALUE_TYPE_LARGE_CHARS:
                l = rawShortValue() & 0xFFFF;
                break;
            default:
                throw new IllegalStateException("not string get");
        }
        for (int i = 0; i < l; i++) {
            if (nextValueCtrlBit() != 0) {
                skipRawShort();
            } else {
                skipRawByte();
            }
        }
        endStringCtrlBits();
        return l;
    }

    public final int stringValue(StringBuilder to) {
        final int t = nextValueType();
        final int l;
        switch (t) {
            case VALUE_TYPE_NULL:
                return 0;
            case VALUE_TYPE_SMALL_CHARS:
                l = next4CtrlBits() + 1;
                break;
            case VALUE_TYPE_LARGE_CHARS:
                l = rawShortValue() & 0xFFFF;
                break;
            default:
                throw new IllegalStateException("not string get");
        }
        for (int i = 0; i < l; i++) {
            if (nextValueCtrlBit() != 0) {
                to.append((char) rawShortValue());
            } else {
                to.append((char) (rawByteValue() & 0xFF));
            }
        }
        endStringCtrlBits();
        return l;
    }

    @Override
    public final int sums() {
        final int t = next4CtrlBits();
        if ((t & ENTRY_TYPE_SUMS) == 0) {
            throw new IllegalStateException("not addSums");
        }
        int count = next4CtrlBits() + 1;
        if ((t & ENTRY_TYPE_SMALL) == 0) {
            count += (rawByteValue() & 0xFF) << 4;
        }
        return count;

    }

    public final int intSum() {
        final int t = next4CtrlBits();
        switch (t) {
            case SUM_TYPE_BYTE:
                return rawByteValue();
            case SUM_TYPE_SHORT:
                return rawShortValue();
            case SUM_TYPE_INT:
                return rawIntValue();
            case SUM_TYPE_INT_48:
                break;
            case SUM_TYPE_LONG:
                final long m = rawLongValue();
                if (Integer.MIN_VALUE <= m && m <= Integer.MAX_VALUE) {
                    return (int) m;
                }
            default:
                return t - SUM_TYPE_0;
        }
        throw new IllegalStateException("not int get");
    }

    public final long longSum() {
        final int t = next4CtrlBits();
        switch (t) {
            case SUM_TYPE_BYTE:
                return rawByteValue();
            case SUM_TYPE_SHORT:
                return rawShortValue();
            case SUM_TYPE_INT:
                return rawIntValue();
            case SUM_TYPE_INT_48:
                return rawInt48Value();
            case SUM_TYPE_LONG:
                return rawLongValue();
            default:
                return t - SUM_TYPE_0;
        }
    }


    private final static int BEGIN_BOOL_BITS = 0xFF;
    private final static int END_BOOL_BITS = BEGIN_BOOL_BITS << 8;
    private long kPtr;
    private int kLen;
    private int kOffset;
    private int keyBoolBits;

    private long ensureKeyBytes(int size) {
        keyBoolBits = END_BOOL_BITS;
        final int oldOffset = kOffset;
        if ((kOffset = oldOffset + size) > kLen) {
            throw new IllegalStateException("end of buffer!");
        }
        return kPtr + oldOffset;
    }

    public final boolean booleanKey() {
        if (keyBoolBits == END_BOOL_BITS) {
            keyBoolBits = (byteKey() & 0xFF) << 24 | BEGIN_BOOL_BITS;
        }
        final boolean bit = keyBoolBits < 0;
        keyBoolBits <<= 1;
        return bit;
    }


    public final byte byteKey() {
        return (byte) (Unsf.theUnsafe.getByte(ensureKeyBytes(1)) + Byte.MIN_VALUE);
    }

    public final short shortKey() {
        return (short) (Short.reverseBytes(Unsf.theUnsafe.getShort(ensureKeyBytes(2))) + Short.MIN_VALUE);
    }

    public final int intKey() {
        return Integer.reverseBytes(Unsf.theUnsafe.getInt(ensureKeyBytes(4))) + Integer.MIN_VALUE;
    }

    public final long longKey() {
        return Long.reverseBytes(Unsf.theUnsafe.getLong(ensureKeyBytes(8))) + Long.MIN_VALUE;
    }

    public final String stringKey() {
        final StringBuilder sb = memTable.ensureSSB();
        sb.setLength(0);
        stringKey(sb);
        return sb.toString();
    }

    @Override
    public final int compareKey(CharSequence key) {
        final int end = kLen - 1;
        final int l = key.length();
        int i = kOffset;
        int j = 0;
        while (i < end) {
            final char c = Unsf.theUnsafe.getChar(kPtr + i);
            if (c == '\0') {
                kOffset = i + 2;
                keyBoolBits = END_BOOL_BITS;
                if (l == j) {
                    return 0;
                }
                return -1;
            }
            if (j == l) {
                return 1;
            }
            final int cm = c - key.charAt(j++);
            if (cm != 0) {
                return cm;
            }
            i += 2;
        }
        throw new IllegalStateException("invalid string key!");
    }

    @Override
    public final int compareKey(String key) {
        final char[] chars = Unsf.getStringChars(Objects.requireNonNull(key));
        final int end = kLen - 1;
        int i = kOffset;
        int j = 0;
        while (i < end) {
            final char c = Unsf.theUnsafe.getChar(kPtr + i);
            if (c == '\0') {
                kOffset = i + 2;
                keyBoolBits = END_BOOL_BITS;
                if (chars.length == j) {
                    return 0;
                }
                return -1;
            }
            if (j == chars.length) {
                return 1;
            }
            final int cm = c - chars[j++];
            if (cm != 0) {
                return cm;
            }
            i += 2;
        }
        throw new IllegalStateException("invalid string key!");
    }

    public final int stringKey(StringBuilder to) {
        final int end = kLen - 1;
        int i = kOffset;
        while (i < end) {
            final char c = Unsf.theUnsafe.getChar(kPtr + i);
            if (c == '\0') {
                final int size = (i - kOffset) / 2;
                kOffset = i + 2;
                keyBoolBits = END_BOOL_BITS;
                return size;
            }
            to.append(Character.reverseBytes(c));
            i += 2;
        }
        throw new IllegalStateException("invalid string key!");
    }

    @Override
    public final boolean keyEOF() {
        return kOffset >= kLen;
    }

    final KVBufferImpl resetFromDB(long kPtr, int kLen, long vPtr, int vLen) {
        reset(kPtr, kLen, vPtr, vLen, 0);
        return this;
    }

    final KVBufferImpl resetFromDB(long kPtr, int kLen, long result) {
        reset(kPtr, kLen, Unsf.theUnsafe.getAddress(result + 8), Unsf.theUnsafe.getInt(result + 16), 0);
        return this;
    }

    final KVBufferImpl resetFromMM(long kPtr, int kLen, long vPtr, int vLen, int vNext) {
        reset(kPtr, kLen, vPtr + CHUNK_HEAD_SIZE, vLen, vNext);
        return this;
    }

    final KVBufferImpl resetFromMM(long kPtr, int kLen, long vPtr) {
        reset(kPtr, kLen, vPtr + CHUNK_HEAD_SIZE, getValueChunkUsed(vPtr), getValueChunkNext(vPtr));
        return this;
    }
}
