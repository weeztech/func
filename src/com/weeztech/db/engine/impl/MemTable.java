package com.weeztech.db.engine.impl;

import com.weeztech.db.engine.DBWriter;
import com.weeztech.db.engine.KVDecoder;
import com.weeztech.utils.Unsf;
import org.rocksdb.*;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gaojingxin on 15/3/28.
 */
final class MemTable extends MemTool implements DBWriter {
    final static int RD_RESULT_SIZE = 24;
    final static int RD_RESULTS_SIZE = RD_RESULT_SIZE * 2;
    final static int MAX_LEVELS = 20;
    final static int OFFSET_SIZE = 3;
    final static int ROOT_LEVELS_SIZE = (MAX_LEVELS + 1) * OFFSET_SIZE;

    final static int KEY_BUFFER_SIZE = MAX_KEY_SIZE * 2;
    final static int SHARED_STRING_BUILDER_HOLD_LEN = 4 * 1024;//8K
    final static int WRITE_THREAD_BLOCKS_SIZE = 8 * 1024 * 1024;//128MB
    final static int READ_THREAD_BLOCKS_SIZE = 2 * 1024 * 1024;//2MB

    final static byte STATE_READY = 0;
    final static byte STATE_KV_K = 1;
    final static byte STATE_KV_V = 2;
    final static byte STATE_LOOKUP_KEY = STATE_KV_K + 0x10;
    final static byte STATE_CURSOR_MIN_KEY = STATE_KV_K + 0x20;
    final static byte STATE_CURSOR_MAX_KEY = STATE_KV_K + 0x30;
    final static byte STATE_WRITE_KEY = STATE_KV_K + 0x40;
    final static byte STATE_WRITE_VALUE = STATE_KV_V;
    final static byte STATE_ADD_VALUE = STATE_KV_V + 0x10;
    final static byte STATE_DISPOSED = 0x70;


    final long block;
    final long vResult;
    final long kResult;
    final long levels;
    private final long keyBuf;
    private long ptrSaved;
    private long ptrStart;
    private long ptr;
    private long ptrLimit;
    private long bitsPtr;
    private int kvCount;
    private int bytes;
    private int valueBytes;
    private int fieldCount;
    private int lastSumEntryOffset;
    private byte levelCount;
    private byte state;
    private byte tmpBits;
    private byte bitOffset;
    private byte nullReserved;
    private boolean excludeMinKey;
    private boolean excludeMaxKey;
    private boolean noOldSums;
    private final RocksDB db;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final ReadOptions itrOptions;
    final boolean forReadThread;
    private ColumnFamilyHandle defaultCF;
    private ColumnFamilyHandle tmpCF;
    private ColumnFamilyDescriptor cfd;
    private final int blockSize;

    final byte getLevelCount() {
        return levelCount;
    }

    public void setSnapshot(Snapshot s) {
        this.readOptions.setSnapshot(s);
        this.itrOptions.setSnapshot(s);
    }

    MemTable(RocksDB db, boolean forReadThread) {
        this.db = db;
        this.forReadThread = forReadThread;
        defaultCF = db.getDefaultColumnFamily();
        writeOptions = new WriteOptions();
        readOptions = new ReadOptions();
        itrOptions = new ReadOptions();
        itrOptions.setFillCache(false);
        blockSize = forReadThread ? READ_THREAD_BLOCKS_SIZE : WRITE_THREAD_BLOCKS_SIZE;
        writeOptions.setDisableWAL(forReadThread);
        block = Unsf.allocateMemory(blockSize + ROOT_LEVELS_SIZE + KEY_BUFFER_SIZE + RD_RESULTS_SIZE);
        levels = block + blockSize;
        keyBuf = levels + ROOT_LEVELS_SIZE;
        kResult = keyBuf + KEY_BUFFER_SIZE;
        vResult = kResult + RD_RESULT_SIZE;
        Unsf.theUnsafe.putLong(kResult, 0);
        Unsf.theUnsafe.putLong(vResult, 0);
        ptr = block + 2;
        lastSumEntryOffset = INVALID_OFFSET;
        state = STATE_READY;
    }

    private void resetMem() {
        if (state == STATE_DISPOSED) {
            throw new IllegalStateException();
        }
        ptr = block + 2;
        tryReleaseSSB();
        resetRDResult();
        lastSumEntryOffset = INVALID_OFFSET;
        bytes = kvCount = levelCount = 0;
        state = STATE_READY;
    }

    private void resetRDResult() {
        Weez.freeBufResult(kResult, 2);
    }

    private void resetCF() {
        setSnapshot(null);
        if (forReadThread && tmpCF != null) {
            try {
                db.dropColumnFamily(tmpCF);
            } catch (Throwable e) {
                e.printStackTrace();//TODO log
            }
            tmpCF.dispose();
            releaseTmpCFD(cfd);
            cfd = null;
        }
    }

    public void reset() {
        resetMem();
        resetCF();
    }

    final RocksIterator allocDefaultCFIterator() {
        return db.newIterator(defaultCF, itrOptions);
    }

    final void releaseDefaultCFIterator(RocksIterator ri) {
        ri.dispose();
    }


    static final ConcurrentLinkedQueue<ColumnFamilyDescriptor> tmpCFDCache = new ConcurrentLinkedQueue<>();
    static final AtomicInteger tmpCFDNameSeed = new AtomicInteger();
    static final ColumnFamilyOptions tmpCFDOptions = new ColumnFamilyOptions();

    static ColumnFamilyDescriptor allocTempCFD() {
        final ColumnFamilyDescriptor cfd = tmpCFDCache.poll();
        if (cfd != null) {
            return cfd;
        }
        int id = tmpCFDNameSeed.incrementAndGet();
        final byte[] name;
        if (id < 0xFF) {
            name = new byte[]{(byte) '_', (byte) id};
        } else if (id <= 0xFFFF) {
            name = new byte[]{(byte) '_', (byte) id, (byte) (id >> 8)};
        } else {
            throw new UnsupportedOperationException("too many tmp CFD!");
        }
        return new ColumnFamilyDescriptor(name, tmpCFDOptions);
    }

    static void releaseTmpCFD(ColumnFamilyDescriptor cfd) {
        tmpCFDCache.offer(cfd);
    }

    private void compressSum(long sum) {
        if (Integer.MIN_VALUE <= sum && sum <= Integer.MAX_VALUE) {
            final int v = (int) sum;
            if (Byte.MIN_VALUE <= sum && sum <= Byte.MAX_VALUE) {
                if (MIN_TINY_SUM <= v && v <= MAX_TIME_SUM) {
                    write4CtrlBits(v + SUM_TYPE_0);
                } else {
                    write4CtrlBits(SUM_TYPE_BYTE);
                    writeRawValue((byte) v);
                }
            } else if (Short.MIN_VALUE <= v && v <= Short.MAX_VALUE) {
                write4CtrlBits(SUM_TYPE_SHORT);
                writeRawValue((short) v);
            } else {
                write4CtrlBits(SUM_TYPE_INT);
                writeRawValue(v);
            }
        } else if (MIN_INT_48 <= sum && sum <= MAX_INT_48) {
            write4CtrlBits(SUM_TYPE_INT_48);
            writeRawValueInt48(sum);
        } else {
            write4CtrlBits(SUM_TYPE_LONG);
            writeRawValue(sum);
        }
    }

    private void compressSums(int sumEntryOffset) {
        while (sumEntryOffset != INVALID_OFFSET) {
            final long sumPtr = block + sumEntryOffset;
            sumEntryOffset = getValueChunkNext(sumPtr);
            final int used = getValueChunkUsed(sumPtr);
            if (used != 0) {//not deleted
                ptrLimit = ptr + used;
                long aPtr = sumPtr + CHUNK_HEAD_SIZE;
                int fc = Unsf.theUnsafe.getByte(aPtr++) & 0xFF;
                if ((fc & ENTRY_TYPE_SMALL) == 0) {
                    fc |= (Unsf.theUnsafe.getByte(aPtr++) & 0xFF) << 8;
                }
                fc = (fc >> 4) + 1;
                ptr = aPtr;
                aPtr++;//skip 1 byte value type tag
                bitOffset = 0;
                do {
                    compressSum(Unsf.theUnsafe.getLong(aPtr));
                    if (--fc == 0) {
                        break;
                    }
                    aPtr += 8;
                    compressSum(Unsf.theUnsafe.getLong(aPtr));
                    aPtr += 9;
                } while (--fc > 0);
                if (bitOffset != 0) {
                    flushCtrlBits(tmpBits);
                }
                final int newUsed = (int) ((ptr - sumPtr) - CHUNK_HEAD_SIZE);
                putValueChunkUsed(sumPtr, newUsed);
                bytes += newUsed - used;
            }
        }
    }

    public void commit() throws Throwable {
        if (state == STATE_DISPOSED || state != STATE_READY) {
            throw new IllegalStateException();
        }
        if (kvCount > 0) {
            compressSums(lastSumEntryOffset);
            final WriteBatch wb = new WriteBatch(bytes + kvCount * (forReadThread ? 10 : 9));
            try {
                ColumnFamilyHandle cf;
                if (forReadThread) {
                    if (tmpCF == null) {
                        cfd = allocTempCFD();
                        try {
                            tmpCF = db.createColumnFamily(cfd);
                        } catch (Throwable e) {
                            releaseTmpCFD(cfd);
                            throw e;
                        }
                    }
                    cf = tmpCF;
                } else {
                    cf = defaultCF;
                }
                Weez.wbPutAll(wb, cf, block, block + getBiggerOffset(levels, 0));
                db.write(writeOptions, wb);
            } finally {
                wb.dispose();
            }
            resetMem();
        }
    }

    public MemTable write(short category) {
        if (state != STATE_READY) {
            throw new IllegalStateException();
        }
        final int level = Integer.numberOfTrailingZeros(kvCount + 1);
        final int entryHeadSize = getKeyOffsetByLevel(level);
        if (ptr - block > blockSize) {
            throw new IllegalStateException("out of MemTable memory!");
        }
        putKeyLevel(ptr, level);
        ptrSaved = ptr;
        ptrStart = ptr = ptr + entryHeadSize;
        ptrLimit = ptrStart + MAX_KEY_SIZE;
        state = STATE_WRITE_KEY;
        bitOffset = 0;
        return key(category);
    }

    static IllegalStateException numberOfSumFieldsNotMatch() {
        return new IllegalStateException("number of sum fields not match");
    }

    private void ensurePtrLimit(long limit) {
        if (limit > block + blockSize) {
            throw new IllegalStateException("out of MemTable memory!");
        }
        ptrLimit = limit;
    }

    private void tryPtrLimit(long limit) {
        final long blockEnd = block + blockSize;
        ptrLimit = limit > blockEnd ? blockEnd : limit;
    }

    private void writeEntryHead(int fieldCount, int entryType) {
        if ((entryType & ENTRY_TYPE_SUMS) != 0) {
            state = STATE_ADD_VALUE;
        } else {
            state = STATE_WRITE_VALUE;
            bitOffset = 0;
        }
        fieldCount--;
        if (fieldCount <= 0xF) {
            write4CtrlBits(entryType | ENTRY_TYPE_SMALL);
            write4CtrlBits(fieldCount & 0xF);
        } else {
            write4CtrlBits(entryType);
            write4CtrlBits(fieldCount & 0xF);
            writeRawValue((byte) (fieldCount >> 4));
        }
    }

    private void doEndKey(int fieldCount, int entryType, boolean sumsPut) {
        if (state != STATE_WRITE_KEY) {
            throw new IllegalStateException();
        }
        this.fieldCount = fieldCount;
        final int keyLen = (int) (ptr - ptrStart);
        final byte keyLevel = getKeyLevel(ptrSaved);
        final long keyLevels = ptrSaved + LEVELS_OFFSET;
        final int oldLevelMax = levelCount - 1;
        final long slSavedPtr = keyBuf + MAX_KEY_SIZE;
        if (oldLevelMax >= 0) {
            final long keyPtr = ptrStart;
            long smallerLevels = levels;
            int smallerOffset = 0;
            for (int level = oldLevelMax; ; ) {
                final int biggerOffset = getBiggerOffset(smallerLevels, level);
                if (biggerOffset > 0) {
                    final long biggerEntry = block + biggerOffset;
                    final long biggerKeyPtr = biggerEntry + getKeyOffset(biggerEntry);
                    final int biggerKeyLen = getKeyLen(biggerEntry);
                    final int c = compareKey(keyPtr, keyLen, biggerKeyPtr, biggerKeyLen);
                    if (c == 0) {//exists!
                        //ptrSaved = ptrSaved;
                        long aPtr = biggerKeyPtr + biggerKeyLen;
                        int existsSize;
                        int sumsKind = getSumsKind(biggerEntry);
                        if (sumsKind != 0) {
                            int chunkSize = getValueChunkSize(aPtr);
                            if (chunkSize == 0) {//only once!
                                aPtr = block + getValueChunkNext(aPtr);
                                chunkSize = getValueChunkSize(aPtr);
                            }
                            existsSize = getValueChunkUsed(aPtr);
                            if (fieldCount == 0) {//deleting
                                putValueChunkUsed(aPtr, 0);
                                setSumsKind(biggerEntry, SUMS_KIND_DEL);
                                ptr = ptrSaved;
                                ptrSaved = 0;
                                state = STATE_READY;
                            } else {
                                if ((entryType & ENTRY_TYPE_SUMS) == 0 || chunkSize != sumsChunkSize(fieldCount)) {
                                    throw numberOfSumFieldsNotMatch();
                                }
                                ptrStart = aPtr;
                                ptr = aPtr + CHUNK_HEAD_SIZE;
                                ensurePtrLimit(ptr + chunkSize);
                                if ((noOldSums = (existsSize == 0 || sumsPut)) && sumsKind != SUMS_KIND_PUT) {
                                    setSumsKind(biggerEntry, SUMS_KIND_PUT);
                                }
                                writeEntryHead(fieldCount, entryType);
                            }
                        } else {
                            existsSize = 0;
                            for (; ; ) {
                                final int chunkUsed = getValueChunkUsed(aPtr);
                                if (existsSize == 0 && chunkUsed != 0) {//first used chunk
                                    if ((entryType & ENTRY_TYPE_SUMS) != 0) {
                                        throw numberOfSumFieldsNotMatch();
                                    }
                                    ptrStart = aPtr;
                                    ptr = aPtr + CHUNK_HEAD_SIZE;
                                    ptrLimit = ptr + getValueChunkSize(aPtr);
                                }
                                existsSize += chunkUsed;
                                final int nextOffset = getValueChunkNext(aPtr);
                                if (nextOffset == INVALID_OFFSET) {
                                    if (existsSize == 0) {//deleted
                                        if (fieldCount > 0) {
                                            ptrStart = ptrSaved;
                                            ptrSaved = 0;
                                            ptr = ptrStart + CHUNK_HEAD_SIZE;
                                            final int currOffset = (int) (ptrStart - block);
                                            putValueChunkNext(aPtr, currOffset);
                                            if ((entryType & ENTRY_TYPE_SUMS) != 0) {//addSums,putSums
                                                setSumsKind(biggerEntry, SUMS_KIND_PUT);
                                                final int needChunkSize = sumsChunkSize(fieldCount);
                                                putValueHead(ptrStart, needChunkSize, needChunkSize, lastSumEntryOffset);
                                                lastSumEntryOffset = currOffset;
                                                ensurePtrLimit(ptr + needChunkSize);
                                                noOldSums = true;
                                            } else {//values
                                                tryPtrLimit(ptr + MAX_VALUE_SIZE);
                                            }
                                        } else {//deleting
                                            putValueHead(aPtr, 0, 0);
                                            ptr = ptrSaved;
                                            ptrSaved = 0;
                                            state = STATE_READY;
                                            break;
                                        }
                                    }
                                    writeEntryHead(fieldCount, entryType);
                                    break;
                                }
                                aPtr = block + nextOffset;
                            }
                        }
                        bytes -= existsSize;
                        return;
                    } else if (c > 0) {
                        smallerLevels = biggerEntry + LEVELS_OFFSET;
                        smallerOffset = biggerOffset;
                        continue;
                    }
                }
                if (level <= keyLevel) {
                    putBiggerOffset(keyLevels, level, biggerOffset);
                    putBiggerOffset(slSavedPtr, level, smallerOffset);
                }
                if (--level < 0) {//need insert
                    break;
                }
            }
        }
        final int entryOffset = (int) (ptrSaved - block);
        byte level = keyLevel;
        if (oldLevelMax < keyLevel) {
            putSmallerOffset(levels, entryOffset);
            do {
                putBiggerOffset(keyLevels, level, INVALID_OFFSET);
                putBiggerOffset(levels, level, entryOffset);
                level--;
            } while (level > oldLevelMax);
            levelCount = (byte) (keyLevel + 1);
            if (level < 0) {
                putSmallerOffset(keyLevels, INVALID_OFFSET);
                putSmallerOffset(levels, entryOffset);
            }
        } else {
            levelCount = (byte) (oldLevelMax + 1);
        }
        if (level >= 0) {
            for (; ; ) {
                final int so = getBiggerOffset(slSavedPtr, level);
                putBiggerOffset(so == INVALID_OFFSET ? levels : block + so + LEVELS_OFFSET, level, entryOffset);
                if (level == 0) {
                    putSmallerOffset(keyLevels, so);
                    final int bo = getBiggerOffset(keyLevels, 0);
                    putSmallerOffset(bo == INVALID_OFFSET ? levels : block + bo + LEVELS_OFFSET, entryOffset);
                    break;
                }
                level--;
            }
        }
        final boolean isSums = (entryType & ENTRY_TYPE_SUMS) != 0;
        putKeyLen(ptrSaved, keyLen, isSums ? (sumsPut ? SUMS_KIND_PUT : SUMS_KIND_ADD) : 0);
        bytes += keyLen;
        kvCount++;
        ptrStart = ptr;
        ptr = ptrStart + CHUNK_HEAD_SIZE;
        ptrSaved = 0;
        if (fieldCount == 0) {//deleting
            putEmptyValueHead(ptrStart);
            state = STATE_READY;
            return;
        }
        valueBytes = 0;
        if (isSums) {
            final int needChunkSize = sumsChunkSize(fieldCount);
            ensurePtrLimit(ptr + needChunkSize);
            putValueChunkNext(ptrStart, lastSumEntryOffset);
            lastSumEntryOffset = (int) (ptrStart - block);
            noOldSums = true;
        } else {
            tryPtrLimit(ptr + MAX_VALUE_SIZE);
            putEmptyValueHead(ptrStart);
        }
        writeEntryHead(fieldCount, entryType);
    }

    @Override
    public final MemTable delete() {
        doEndKey(0, 0, false);
        return this;
    }

    public final MemTable values(int fieldCount) {
        if (fieldCount <= 0 && MAX_FIELDS < fieldCount) {
            throw new IllegalArgumentException("fieldCount");
        }
        doEndKey(fieldCount, ENTRY_TYPE_VALUES, false);
        return this;
    }


    public final MemTable addSums(int fieldCount) {
        if (fieldCount <= 0 && MAX_FIELDS < fieldCount) {
            throw new IllegalArgumentException("fieldCount");
        }
        doEndKey(fieldCount, ENTRY_TYPE_SUMS, false);
        return this;
    }

    public final MemTable putSums(int fieldCount) {
        if (fieldCount <= 0 && MAX_FIELDS < fieldCount) {
            throw new IllegalArgumentException("fieldCount");
        }
        doEndKey(fieldCount, ENTRY_TYPE_SUMS, true);
        return this;
    }

    public final MemTable add(long value) {
        if (this.state != STATE_ADD_VALUE) {
            throw new IllegalStateException("not in add mode");
        }
        write4CtrlBits(SUM_TYPE_LONG);
        if (ptrLimit - ptr < 8) {
            nextWriteValueChunk(8);
        }
        if (noOldSums) {
            Unsf.theUnsafe.putLong(ptr, value);
        } else {
            Unsf.theUnsafe.putLong(ptr, Unsf.theUnsafe.getLong(ptr) + value);
        }
        ptr += 8;
        fieldCount--;
        return this;
    }

    private static void putEmptyValueHead(long ptr) {
        Unsf.theUnsafe.putLong(ptr, 0);
    }

    private static void putValueHead(long ptr, int size, int used, int next) {
        Unsf.theUnsafe.putShort(ptr, (short) size);
        Unsf.theUnsafe.putShort(ptr + 2, (short) used);
        putOffset(ptr + 4, next);
    }

    private static void putValueChunkUsed(long ptr, int used) {
        Unsf.theUnsafe.putShort(ptr + 2, (short) used);
    }

    private static void putValueHead(long ptr, int used, int next) {
        Unsf.theUnsafe.putShort(ptr + 2, (short) used);
        putOffset(ptr + 4, next);
    }

    private static void putValueChunkNext(long ptr, int next) {
        putOffset(ptr + 4, next);
    }

    private int nextWriteValueChunk(int minSize) {
        final int chunkUsed = (int) (ptr - ptrStart - CHUNK_HEAD_SIZE);
        final int chunkSize = (int) (ptrLimit - ptrStart - CHUNK_HEAD_SIZE);
        int next = getValueChunkNext(ptrStart);
        valueBytes += chunkUsed;
        for (; ; ) {
            final long nextPtr;
            final int nextChunkSize;
            if (next == 0) {
                if (ptrSaved == 0) {//in new chunk
                    throw new IllegalStateException("too big get!");
                } else {
                    nextPtr = ptrSaved;
                    ptrSaved = 0;
                    next = (int) (nextPtr - block);
                    nextChunkSize = Math.min(MAX_VALUE_SIZE - valueBytes, blockSize - next - CHUNK_HEAD_SIZE);
                    if (nextChunkSize < minSize) {
                        throw new IllegalStateException("too big get!");
                    }
                    putValueChunkNext(nextPtr, 0);
                }
            } else {
                nextPtr = block + next;
                nextChunkSize = getValueChunkSize(nextPtr);
                if (nextChunkSize < minSize) {
                    next = getValueChunkNext(nextPtr);
                    continue;
                }
            }
            putValueHead(ptrStart, chunkSize, chunkUsed, next);
            ptrStart = nextPtr;
            ptr = nextPtr + CHUNK_HEAD_SIZE;
            ptrLimit = ptr + nextChunkSize;
            return nextChunkSize;
        }
    }

    private static void checkPutKeyCap(int rest, int need) {
        if (rest < need) {
            throw new IllegalArgumentException("too big lookup");
        }
    }

    public final MemTable end() {
        if ((state & STATE_KV_V) == 0) {
            throw new IllegalStateException();
        }
        if (fieldCount != 0) {
            throw new IllegalStateException("field count error!");
        }
        if (bitOffset >= 0) {
            flushCtrlBits(tmpBits);
        }
        final int used = (int) ((ptr - ptrStart) - CHUNK_HEAD_SIZE);
        bytes += used + valueBytes;
        if (ptrSaved != 0) {
            putValueHead(ptrStart, used, 0);
            ptr = ptrSaved;
            ptrSaved = 0;
        } else {
            putValueHead(ptrStart, used, used, 0);
        }
        state = STATE_READY;
        return this;
    }

    public final MemTable lookup(short category) {
        if (state != STATE_READY) {
            throw new IllegalStateException();
        }
        ptrSaved = ptr;
        ptr = ptrStart = keyBuf;
        ptrLimit = ptr + MAX_KEY_SIZE;
        state = STATE_LOOKUP_KEY;
        return key(category);
    }

    private static final CursorImpl eofCursor = new CursorImpl<>(null);

    public final MemTable from(short category, boolean exclude) {
        if (state != STATE_READY) {
            throw new IllegalStateException();
        }
        ptrSaved = ptr;
        ptr = keyBuf;
        ptrLimit = ptr + MAX_KEY_SIZE;
        excludeMinKey = exclude;
        state = STATE_CURSOR_MIN_KEY;
        return key(category);
    }


    public final MemTable to(boolean exclude) {
        if (state != STATE_CURSOR_MIN_KEY) {
            throw new IllegalStateException();
        }
        ptrStart = ptr;
        ptrLimit = ptr + MAX_KEY_SIZE;
        excludeMaxKey = exclude;
        state = STATE_CURSOR_MAX_KEY;
        Unsf.theUnsafe.putShort(ptr, Unsf.theUnsafe.getShort(keyBuf));
        ptr += 2;
        return this;
    }

    public final <T> CursorImpl<T> cursor(boolean backward, KVDecoder<T> decoder) {
        if (state != STATE_CURSOR_MAX_KEY) {
            throw new IllegalStateException();
        }
        final int minKeyLen = (int) (ptrStart - keyBuf);
        final int maxKeyLen = (int) (ptr - ptrStart);
        final CursorImpl<T> cursor;
        buildCursor:
        {
            if (maxKeyLen == 2) {
                Unsf.theUnsafe.putShort(ptrStart, Short.reverseBytes((short) (Short.reverseBytes(Unsf.theUnsafe.getShort(ptrStart)) + 1)));
                excludeMaxKey = true;
            }
            final int c = compareKey(keyBuf, minKeyLen, ptrStart, maxKeyLen);
            if (c > 0) {
                cursor = eofCursor;
                break buildCursor;
            } else if (c == 0) {
                if (excludeMaxKey || excludeMinKey) {
                    cursor = eofCursor;
                    break buildCursor;
                } else {
                    state = STATE_LOOKUP_KEY;
                    return new CursorImpl<>(get(decoder));
                }
            }
            cursor = new CursorImpl<>(this, decoder, keyBuf, minKeyLen, excludeMinKey, ptrStart, maxKeyLen, excludeMaxKey, backward);
        }
        ptr = ptrSaved;
        ptrSaved = 0;
        state = STATE_READY;
        return cursor;
    }

    private StringBuilder ssb;

    final StringBuilder ensureSSB() {
        if (ssb == null) {
            ssb = new StringBuilder(SHARED_STRING_BUILDER_HOLD_LEN);
        }
        return ssb;
    }

    private void tryReleaseSSB() {
        if (ssb != null && ssb.capacity() > SHARED_STRING_BUILDER_HOLD_LEN) {
            ssb = null;
        }
    }

    private KVBufferImpl bufferCache0;
    private KVBufferImpl bufferCache1;


    final KVBufferImpl allocKVBuffer() {
        final KVBufferImpl b;
        if (bufferCache1 != null) {
            b = bufferCache1;
            bufferCache1 = null;
        } else if (bufferCache0 != null) {
            b = bufferCache0;
            bufferCache0 = null;
        } else {
            b = new KVBufferImpl(this);
        }
        return b;
    }

    final void recycleKVBuffer(KVBufferImpl buffer) {
        if (bufferCache0 == null) {
            bufferCache0 = buffer;
        } else {
            bufferCache1 = buffer;
        }
    }

    private static final KVDecoder<Boolean> valueExist = b -> Boolean.TRUE;

    @Override
    public final boolean exist() {
        return get(valueExist) == Boolean.TRUE;
    }

    @Override
    public final <T> T get(KVDecoder<T> decoder) {
        if (state != STATE_LOOKUP_KEY) {
            throw new IllegalStateException();
        }
        final T result;
        doGet:
        {
            final long keyPtr = ptrStart;
            final int keyLen = (int) (ptr - ptrStart);
            try {
                if (!forReadThread) {
                    long smallerLevels = levels;
                    for (int level = levelCount - 1; level >= 0; level--) {
                        final int entryOffset = getBiggerOffset(smallerLevels, level);
                        if (entryOffset > 0) {
                            final long etrPtr = block + entryOffset;
                            final long lastKeyPtr = etrPtr + getKeyOffset(etrPtr);
                            final int lastKeyLen = getKeyLen(etrPtr);
                            final int c = compareKey(keyPtr, keyLen, lastKeyPtr, lastKeyLen);
                            if (c == 0) {//exists!
                                final long valuePtr = lastKeyPtr + lastKeyLen;
                                final int used = getValueChunkUsed(valuePtr);
                                final int next = getValueChunkNext(valuePtr);
                                final int sumsKind = getSumsKind(etrPtr);
                                switch (sumsKind) {
                                    case 0:
                                        if ((used | next) != 0) {
                                            break;
                                        }
                                    case SUMS_KIND_DEL:
                                        result = null;
                                        break doGet;
                                }
                                final KVBufferImpl buffer = allocKVBuffer().resetFromMM(lastKeyPtr, lastKeyLen, valuePtr, used, next);
                                if (sumsKind == SUMS_KIND_ADD) {
                                    if (Weez.dbGet(db, defaultCF, readOptions, keyPtr, keyLen, vResult)) {
                                        buffer.addSums(vResult);
                                    }
                                    setSumsKind(etrPtr, SUMS_KIND_PUT);
                                }
                                result = decoder.decode(buffer);
                                recycleKVBuffer(buffer);
                                break doGet;
                            } else if (c > 0) {
                                smallerLevels = etrPtr + LEVELS_OFFSET;
                            }
                        }
                    }
                }
                if (Weez.dbGet(db, defaultCF, readOptions, keyPtr, keyLen, vResult)) {
                    final KVBufferImpl buffer = allocKVBuffer();
                    result = decoder.decode(buffer.resetFromDB(keyPtr, keyLen, vResult));
                    recycleKVBuffer(buffer);
                } else {
                    result = null;
                }
            } catch (Throwable e) {
                throw Unsf.throwException(e);
            }
        }
        ptr = ptrSaved;
        ptrSaved = 0;
        state = STATE_READY;
        return result;
    }

    public final void dispose() {
        if (state != STATE_DISPOSED) {
            resetRDResult();
            Unsf.freeMemory(block);
            resetCF();
            writeOptions.dispose();
            readOptions.dispose();
            defaultCF.dispose();
            state = STATE_DISPOSED;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        this.dispose();
        super.finalize();
    }

    @Override
    public MemTable key(boolean key) {
        needKeyState();
        if (bitOffset == 0) {
            if (ptrLimit <= ptr) {
                checkPutKeyCap(0, 1);
            }
            tmpBits = key ? (byte) 0x80 : 0;
            Unsf.theUnsafe.putByte(ptr, tmpBits);
            bitOffset = 7;
            ptr++;
        } else {
            bitOffset--;
            if (key) {
                tmpBits |= 1 << bitOffset;
                Unsf.theUnsafe.putByte(ptr - 1, tmpBits);
            }
        }
        return this;
    }

    @Override
    public MemTable key(byte key) {
        needKeyState();
        if (ptrLimit <= ptr) {
            checkPutKeyCap(0, 1);
        }
        Unsf.theUnsafe.putByte(ptr, (byte) (key - Byte.MIN_VALUE));
        ptr++;
        bitOffset = 0;
        return this;
    }

    @Override
    public MemTable key(short key) {
        needKeyState();
        final int rest = (int) (ptrLimit - ptr);
        if (rest < 2) {
            checkPutKeyCap(rest, 2);
        }
        Unsf.theUnsafe.putShort(ptr, Short.reverseBytes((short) (key - Short.MIN_VALUE)));
        ptr += 2;
        bitOffset = 0;
        return this;
    }

    @Override
    public MemTable key(int key) {
        needKeyState();
        final int rest = (int) (ptrLimit - ptr);
        if (rest < 4) {
            checkPutKeyCap(rest, 4);
        }
        Unsf.theUnsafe.putInt(ptr, Integer.reverseBytes(key - Integer.MIN_VALUE));
        ptr += 4;
        bitOffset = 0;
        return this;
    }

    @Override
    public MemTable key(long key) {
        needKeyState();
        final int rest = (int) (ptrLimit - ptr);
        if (rest < 8) {
            checkPutKeyCap(rest, 8);
        }
        Unsf.theUnsafe.putLong(ptr, Long.reverseBytes(key - Long.MIN_VALUE));
        ptr += 8;
        bitOffset = 0;
        return this;
    }

    @Override
    public MemTable key(CharSequence key) {
        needKeyState();
        if (key instanceof String) {
            keyString((String) key);
        } else {
            final int rest = (int) (ptrLimit - ptr);
            final int len = key.length();
            checkPutKeyCap(rest, (len + 1) * 2);
            for (int i = 0; i < len; i++) {
                final char c = key.charAt(i);
                if (c == '\0') {
                    break;
                }
                Unsf.theUnsafe.putChar(ptr, Character.reverseBytes(c));
                ptr += 2;
            }
            Unsf.theUnsafe.putChar(ptr, '\0');
            ptr += 2;
            bitOffset = 0;
        }
        return this;
    }

    @Override
    public MemTable key(String key) {
        needKeyState();
        keyString(key);
        return this;
    }

    private void keyString(String key) {
        final int rest = (int) (ptrLimit - ptr);
        final int len = key.length();
        checkPutKeyCap(rest, (len + 1) * 2);
        final char[] chars = Unsf.getStringChars(key);
        for (int i = 0; i < len; i++) {
            final char c = chars[i];
            if (c == '\0') {
                break;
            }
            Unsf.theUnsafe.putChar(ptr, Character.reverseBytes(c));
            ptr += 2;
        }
        Unsf.theUnsafe.putChar(ptr, '\0');
        ptr += 2;
        bitOffset = 0;
    }

    private void writeRawValue(byte v) {
        if (ptr == ptrLimit) {
            nextWriteValueChunk(1);
        }
        Unsf.theUnsafe.putByte(ptr, v);
        ptr++;
    }

    private void writeRawValue(short v) {
        if (ptrLimit - ptr < 2) {
            nextWriteValueChunk(2);
        }
        Unsf.theUnsafe.putShort(ptr, v);
        ptr += 2;
    }

    private void writeRawValue(int v) {
        if (ptrLimit - ptr < 4) {
            nextWriteValueChunk(4);
        }
        Unsf.theUnsafe.putInt(ptr, v);
        ptr += 4;
    }

    private void writeRawValue(long v) {
        if (ptrLimit - ptr < 8) {
            nextWriteValueChunk(8);
        }
        Unsf.theUnsafe.putLong(ptr, v);
        ptr += 8;
    }

    private void writeRawValueInt48(long v) {
        if (ptrLimit - ptr < 6) {
            nextWriteValueChunk(6);
        }
        Unsf.theUnsafe.putInt(ptr, (int) v);
        ptr += 4;
        Unsf.theUnsafe.putShort(ptr, (short) (v >> 32));
        ptr += 2;
    }

    private void needKeyState() {
        if ((this.state & STATE_KV_K) == 0) {
            throw new IllegalStateException("not in key put mode");
        }
    }

    private void reserveCtrlBits() {
        if (ptrLimit <= ptr) {
            nextWriteValueChunk(1);
        }
        bitsPtr = ptr++;
    }

    private void flushCtrlBits(byte bits) {
        Unsf.theUnsafe.putByte(bitsPtr, bits);
    }

    private void writeChar(char c) {
        final boolean small = c <= 0xFF;
        byte bits = small ? 0 : (byte) 1;
        if (bitOffset == 0) {
            reserveCtrlBits();
            tmpBits = bits;
            bitOffset = 1;
        } else {
            bits = (byte) ((bits << bitOffset) | tmpBits);
            if (++bitOffset == 8) {
                flushCtrlBits(bits);
                bitOffset = 0;
            } else {
                tmpBits = bits;
            }
        }
        if (small) {
            writeRawValue((byte) c);
        } else {
            writeRawValue((short) c);
        }
    }

    private void adjCharBits() {
        if (bitOffset != 0) {
            if (bitOffset < 4) {
                bitOffset = 4;
            } else if (bitOffset > 4) {
                flushCtrlBits(tmpBits);
                bitOffset = 0;
            }
        }
    }

    @Override
    public final MemTable valueNull() {
        if (this.state != STATE_WRITE_VALUE) {
            throw new IllegalStateException("not in write mode");
        }
        if (bitOffset == 0) {
            if (nullReserved > 0) {
                tmpBits++;
                if (--nullReserved == 0) {
                    bitOffset = 4;
                }
            } else {
                reserveCtrlBits();
                nullReserved = VALUE_TYPE_NULL_MAX;
                tmpBits = VALUE_TYPE_NULL;
            }
        } else {
            assert bitOffset == 4;
            if (nullReserved > 0) {
                tmpBits += 0x10;
                if (--nullReserved == 0) {
                    flushCtrlBits(tmpBits);
                    bitOffset = 0;
                }
            } else {
                nullReserved = VALUE_TYPE_NULL_MAX;
            }
        }
        fieldCount--;
        return this;
    }

    private void write4CtrlBits(int bits) {
        if (bitOffset == 0) {
            if (nullReserved == 0) {
                reserveCtrlBits();
                tmpBits = (byte) bits;
                bitOffset = 4;
            } else {
                flushCtrlBits((byte) ((bits << 4) | tmpBits));
                nullReserved = bitOffset = 0;
            }
        } else {
            assert bitOffset == 4;
            if (nullReserved == 0) {
                flushCtrlBits((byte) ((bits << 4) | tmpBits));
                bitOffset = 0;
            } else {
                flushCtrlBits(tmpBits);
                reserveCtrlBits();
                tmpBits = (byte) bits;
                bitOffset = 4;
                nullReserved = 0;
            }
        }
    }

    private void writeValueType(int t) {
        if (this.state != STATE_WRITE_VALUE) {
            throw new IllegalStateException("not in write mode");
        }
        write4CtrlBits(t);
        fieldCount--;
    }

    @Override
    public final MemTable value(boolean value) {
        writeValueType(value ? VALUE_TYPE_1 : VALUE_TYPE_0);
        return this;
    }


    @Override
    public final MemTable value(int value) {
        if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
            if (-1 <= value && value <= 1) {
                writeValueType(value + VALUE_TYPE_0);
            } else {
                writeValueType(VALUE_TYPE_BYTE);
                writeRawValue((byte) value);
            }
        } else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
            writeValueType(VALUE_TYPE_SHORT);
            writeRawValue((short) value);
        } else {
            writeValueType(VALUE_TYPE_INT);
            writeRawValue(value);
        }
        return this;
    }

    @Override
    public final MemTable value(long value) {
        if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
            final int v = (int) value;
            if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
                if (-1 <= v && v <= 1) {
                    writeValueType(v + VALUE_TYPE_0);
                } else {
                    writeValueType(VALUE_TYPE_BYTE);
                    writeRawValue((byte) v);
                }
            } else if (Short.MIN_VALUE <= v && v <= Short.MAX_VALUE) {
                writeValueType(VALUE_TYPE_SHORT);
                writeRawValue((short) v);
            } else {
                writeValueType(VALUE_TYPE_INT);
                writeRawValue(v);
            }
        } else if (MIN_INT_48 <= value && value <= MAX_INT_48) {
            writeValueType(VALUE_TYPE_INT_48);
            writeRawValueInt48(value);
        } else {
            writeValueType(VALUE_TYPE_LONG);
            writeRawValue(value);
        }
        return this;
    }

    private void writeCharsLen(int l) {
        if (l <= 0x10) {//1~0x10
            writeValueType(VALUE_TYPE_SMALL_CHARS);
            write4CtrlBits(l - 1);
        } else if (l <= 0xFFFF) {
            writeValueType(VALUE_TYPE_LARGE_CHARS);
            writeRawValue((short) l);
        } else {
            throw new IllegalArgumentException("too long string!");
        }
    }

    @Override
    public final MemTable value(CharSequence value) {
        if (value instanceof String) {
            return value((String) value);
        } else {
            final int l = value != null ? value.length() : 0;
            if (l == 0) {
                return valueNull();
            } else {
                writeCharsLen(l);
            }
            for (int i = 0; i < l; i++) {
                writeChar(value.charAt(i));
            }
            adjCharBits();
            return this;
        }
    }

    @Override
    public MemTable value(String value) {
        final int l = value != null ? value.length() : 0;
        if (l == 0) {
            return valueNull();
        }
        writeCharsLen(l);
        final char[] chars = Unsf.getStringChars(value);
        for (int i = 0; i < l; i++) {
            writeChar(chars[i]);
        }
        adjCharBits();
        return this;
    }

    public MemTable value(CharSequence value, int start, int end) {
        if (value instanceof String) {
            return value((String) value, start, end);
        }
        if (start < 0)
            throw new StringIndexOutOfBoundsException(start);
        if (value == null) {
            if (end != 1) {
                throw new StringIndexOutOfBoundsException(end);
            }
            if (start > 0) {
                throw new StringIndexOutOfBoundsException(-start);
            }
            return valueNull();
        }
        if (end > value.length())
            throw new StringIndexOutOfBoundsException(end);
        if (start > end)
            throw new StringIndexOutOfBoundsException(end - start);
        final int l = end - start - 1;
        if (l == 0) {
            return valueNull();
        } else {
            writeCharsLen(l);
        }
        do {
            writeChar(value.charAt(start++));
        } while (start < end);
        adjCharBits();
        return this;
    }

    public MemTable value(String value, int start, int end) {
        if (start < 0)
            throw new StringIndexOutOfBoundsException(start);
        if (value == null) {
            if (end != 1) {
                throw new StringIndexOutOfBoundsException(end);
            }
            if (start > 0) {
                throw new StringIndexOutOfBoundsException(-start);
            }
            return valueNull();
        }
        if (end > value.length())
            throw new StringIndexOutOfBoundsException(end);
        if (start > end)
            throw new StringIndexOutOfBoundsException(end - start);
        final int l = end - start - 1;
        if (l == 0) {
            return valueNull();
        }
        final char[] chars = Unsf.getStringChars(value);
        writeCharsLen(l);
        do {
            writeChar(chars[start++]);
        } while (start < end);
        adjCharBits();
        return this;
    }

    public final MemTable valueTuple(int fields) {
        if (fields < 0 || MAX_FIELDS < fields) {
            throw new IllegalArgumentException("fields");
        }
        if (fields == 0) {
            return valueNull();
        }
        fieldCount += fields--;
        if (fields <= 0xF) {
            writeValueType(VALUE_TYPE_SMALL_TUPLE);
            write4CtrlBits(fields);
        } else {
            writeValueType(VALUE_TYPE_TUPLE);
            write4CtrlBits(fields & 0xF);
            writeRawValue((byte) (fields >> 4));
        }
        return this;
    }

}