package com.weeztech.db.engine.impl;

import com.weeztech.db.engine.Cursor;
import com.weeztech.db.engine.KVDecoder;
import com.weeztech.utils.Unsf;
import org.rocksdb.RocksIterator;
import org.rocksdb.Weez;

/**
 * Created by gaojingxin on 15/4/21.
 */
final class CursorImpl<T> extends MemTool implements Cursor<T> {
    private final MemTable mm;
    private final byte step;
    private final KVDecoder<T> decoder;
    private long mmKey;
    private int mmSmallerOffset;
    private int mmKeyLen;
    private RocksIterator ri;
    private long dbKey;
    private int dbKeyLen;
    private long dbValue;
    private int dbValueLen;
    private boolean manageUntilKey;
    private int untilKeyLen;
    private long untilKey;
    private boolean excludeUntil;
    private T preFetch;
    private int remain = -1;

    private void releaseRI() {
        if (ri != null) {
            mm.releaseDefaultCFIterator(ri);
            ri = null;
        }
        if (mmKeyLen == 0) {
            tryFreeUntilKey();
        }
    }

    private void dbAssignKV(long kvResult) {
        long ptr = kvResult + 8;
        dbKey = Unsf.theUnsafe.getLong(ptr);
        ptr += 8;
        dbKeyLen = Unsf.theUnsafe.getInt(ptr);
        ptr += 16;
        dbValue = Unsf.theUnsafe.getLong(ptr);
        ptr += 8;
        dbValueLen = Unsf.theUnsafe.getInt(ptr);
    }


    private T fetchNext() {
        for (T next; ; ) {
            final boolean mmOrDB;
            final boolean dbNext;
            if (ri != null) {
                if (mmKeyLen > 0) {
                    final int c = compareKey(mmKey, mmKeyLen, dbKey, dbKeyLen);
                    if (c == 0) {
                        mmOrDB = true;
                        dbNext = true;
                    } else if ((step ^ c) < 0) {
                        mmOrDB = true;
                        dbNext = false;
                    } else {
                        mmOrDB = false;
                        dbNext = true;
                    }
                } else {
                    mmOrDB = false;
                    dbNext = true;
                }
            } else if (mmKeyLen > 0) {
                mmOrDB = true;
                dbNext = false;
            } else {
                return null;
            }
            fetch:
            {
                final KVBufferImpl buffer;
                if (mmOrDB) {
                    final long chunkPtr = mmKey + mmKeyLen;
                    final int nextChunk = getValueChunkNext(chunkPtr);
                    final int used = getValueChunkUsed(chunkPtr);
                    final long entryPtr = mmKey + mmSmallerOffset - LEVELS_OFFSET;
                    final int sumsKind = getSumsKind(entryPtr);
                    switch (sumsKind) {
                        case 0:
                            if ((used | nextChunk) != 0) {
                                break;
                            }
                        case SUMS_KIND_DEL:
                            next = null;
                            break fetch;
                    }
                    buffer = mm.allocKVBuffer().resetFromMM(mmKey, mmKeyLen, chunkPtr, used, nextChunk);
                    if (dbNext && sumsKind == SUMS_KIND_ADD) {
                        buffer.addSums(dbValue, dbValueLen);
                        setSumsKind(entryPtr, SUMS_KIND_PUT);
                    }
                    mmMove(step);
                } else {
                    buffer = mm.allocKVBuffer().resetFromDB(dbKey, dbKeyLen, dbValue, dbValueLen);
                }
                if (remain >= 0) {
                    if (decoder.test(buffer)) {
                        remain++;
                    }
                    next = null;
                } else {
                    next = decoder.decode(buffer);
                }
                mm.recycleKVBuffer(buffer);
            }
            if (dbNext) {
                dbMoveNext();
            }
            if (next != null) {
                return next;
            }
        }
    }

    private static final int MOVE_CURSOR_TO_MIN = -2;
    private static final int MOVE_CURSOR_TO_SMALLER = -1;
    private static final int MOVE_CURSOR_TO_BIGGER = 1;
    private static final int MOVE_CURSOR_TO_MAX = 2;


    private boolean mmMove(int mode) {
        doMove:
        {
            if (mm.getLevelCount() == 0) {
                mmKeyLen = 0;
                break doMove;
            }
            final int entryOffset;
            switch (mode) {
                case MOVE_CURSOR_TO_SMALLER:
                    entryOffset = getSmallerOffset(mmKey + mmSmallerOffset);
                    break;
                case MOVE_CURSOR_TO_BIGGER:
                    entryOffset = getBiggerOffset(mmKey + mmSmallerOffset, 0);
                    break;
                case MOVE_CURSOR_TO_MAX:
                    entryOffset = getSmallerOffset(mm.levels);
                    break;
                case MOVE_CURSOR_TO_MIN:
                    entryOffset = getBiggerOffset(mm.levels, 0);
                    break;
                default:
                    throw new IllegalArgumentException("mode");
            }
            if (entryOffset == 0) {
                mmKeyLen = 0;
                break doMove;
            }
            final long entryPtr = mm.block + entryOffset;
            final int keyOffset = getKeyOffset(entryPtr);
            mmKey = entryPtr + keyOffset;
            mmKeyLen = getKeyLen(entryPtr);
            if (untilKeyLen > 0) {
                final int c = compareKey(mmKey, mmKeyLen, untilKey, untilKeyLen);
                if (c == 0) {
                    if (excludeUntil) {
                        mmKeyLen = 0;
                        break doMove;
                    }
                } else if ((c ^ step)>=0) {
                    mmKeyLen = 0;
                    break doMove;
                }
            }
            mmSmallerOffset = LEVELS_OFFSET - keyOffset;
        }
        if (mmKeyLen == 0) {
            if (ri == null) {
                tryFreeUntilKey();
            }
            return false;
        }
        return true;
    }

    private int mmSeek(long keyPtr, int keyLen) {
        long smallerLevels = mm.levels;
        for (int level = mm.getLevelCount() - 1; level >= 0; ) {
            final int entryOffset = getBiggerOffset(smallerLevels, level);
            if (entryOffset > 0) {
                final long entryPtr = mm.block + entryOffset;
                final int keyOffset = getKeyOffset(entryPtr);
                final long lastKeyPtr = entryPtr + keyOffset;
                final int lastKeyLen = getKeyLen(entryPtr);
                final int c = compareKey(lastKeyPtr, lastKeyLen, keyPtr, keyLen);
                if (c < 0) {
                    smallerLevels = entryPtr + LEVELS_OFFSET;
                    continue;
                }
                if (c == 0 || --level < 0) {
                    mmSmallerOffset = LEVELS_OFFSET - keyOffset;
                    mmKey = lastKeyPtr;
                    mmKeyLen = lastKeyLen;
                    return c;
                }
            } else if (--level < 0) {
                break;
            }
        }
        return -1;
    }

    private boolean mmSeek(long keyMin, int keyMinLen, boolean excludeMin,
                           long keyMax, int keyMaxLen, boolean excludeMax, boolean backward) {
        if (mm.forReadThread) {
            mmKeyLen = 0;
            return false;
        }
        final int seekMode;
        if (backward) {
            if (keyMaxLen == 0) {
                seekMode = MOVE_CURSOR_TO_MAX;
            } else {
                final int c = mmSeek(keyMax, keyMaxLen);
                if (c < 0) {//无效
                    seekMode = MOVE_CURSOR_TO_MAX;
                } else if (c > 0 || excludeMax) {
                    seekMode = MOVE_CURSOR_TO_SMALLER;
                } else {
                    return true;
                }
            }
        } else {
            if (keyMinLen == 0) {
                seekMode = MOVE_CURSOR_TO_MIN;
            } else {
                final int c = mmSeek(keyMin, keyMinLen);
                if (c < 0) {//无效
                    mmKeyLen = 0;
                    return false;
                } else if (c == 0 && excludeMin) {
                    seekMode = MOVE_CURSOR_TO_BIGGER;
                } else if (keyMaxLen > 0) {
                    final int c2 = compareKey(mmKey, mmKeyLen, keyMax, keyMaxLen);
                    if (c2 > 0 || ((c2 == 0) && excludeMax)) {
                        mmKeyLen = 0;
                        return false;
                    }
                    return true;
                } else {
                    return true;
                }
            }
        }
        return mmMove(seekMode);
    }

    private boolean dbMoveNext() {
        try {
            if (Weez.riStep(ri, step, untilKey, untilKeyLen, excludeUntil, mm.kResult, mm.vResult)) {
                dbAssignKV(mm.kResult);
                return true;
            }
        } catch (Throwable e) {
            releaseRI();
            throw Unsf.throwException(e);
        }
        releaseRI();
        return false;
    }

    private boolean dbSeek(long keyMin, int keyMinLen, boolean excludeMin,
                           long keyMax, int keyMaxLen, boolean excludeMax, boolean backward) {
        final RocksIterator ri = mm.allocDefaultCFIterator();
        try {
            if (Weez.riSeek(ri, keyMin, keyMinLen, excludeMin, keyMax, keyMaxLen, excludeMax, backward, mm.kResult, mm.vResult)) {
                dbAssignKV(mm.kResult);
                this.ri = ri;
                return true;
            }
        } catch (Throwable e) {
            mm.releaseDefaultCFIterator(ri);
            throw Unsf.throwException(e);
        }
        mm.releaseDefaultCFIterator(ri);
        return false;
    }

    CursorImpl(MemTable mm, KVDecoder<T> decoder, long keyMin, int keyMinLen, boolean excludeMin,
               long keyMax, int keyMaxLen, boolean excludeMax, boolean backward) {
        this.mm = mm;
        this.decoder = decoder;
        if (backward) {
            step = MOVE_CURSOR_TO_SMALLER;
            untilKey = keyMin;
            untilKeyLen = keyMinLen;
            excludeUntil = excludeMin;
        } else {
            step = MOVE_CURSOR_TO_BIGGER;
            untilKey = keyMax;
            untilKeyLen = keyMaxLen;
            excludeUntil = excludeMax;
        }
        if ((dbSeek(keyMin, keyMinLen, excludeMin, keyMax, keyMaxLen, excludeMax, backward) | // must be |
                mmSeek(keyMin, keyMinLen, excludeMin, keyMax, keyMaxLen, excludeMax, backward))
                && untilKeyLen > 0) {
            untilKey = Unsf.cloneMemory(untilKey, untilKeyLen);
            manageUntilKey = true;
        }
    }

    CursorImpl(T next) {
        preFetch = next;
        mm = null;
        decoder = null;
        step = 0;
    }

    @Override
    protected final void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    private void tryFreeUntilKey() {
        if (manageUntilKey) {
            manageUntilKey = false;
            Unsf.freeMemory(untilKey);
        }
    }

    @Override
    public final void close() {
        mmKeyLen = 0;
        releaseRI();
    }

    @Override
    public final boolean hasNext() {
        if (preFetch == null) {
            preFetch = fetchNext();
        }
        return preFetch != null;
    }

    @Override
    public final int remain() {
        if (remain < 0) {
            if (preFetch != null) {
                remain = 1;
                preFetch = null;
            } else {
                remain = 0;
            }
            fetchNext();
        }
        return remain;
    }

    @Override
    public final T next() {
        if (preFetch == null) {
            return fetchNext();
        } else {
            final T n = preFetch;
            preFetch = null;
            return n;
        }
    }
}
