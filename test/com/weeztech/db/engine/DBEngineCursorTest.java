package com.weeztech.db.engine;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created by gaojingxin on 15/5/1.
 */
public class DBEngineCursorTest {
    private static final short category = 2;
    @ClassRule
    public static final DBEngineResource db = new DBEngineResource();


    static class KV {
        final int k;
        final int v;

        @Override
        public String toString() {
            return k + "->" + v;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof KV) {
                final KV other = (KV) obj;
                return other.k == k && other.v == v;
            }
            return false;
        }

        KV(int k, int v) {
            this.k = k;
            this.v = v;
        }
    }

    final KV[] kvs1;
    final KV[] kvs2;

    public DBEngineCursorTest() {
        kvs1 = new KV[]{
                new KV(1, 1),
                new KV(2, 2),
                new KV(3, 3),
                new KV(4, 4),
                new KV(5, 5),
        };
        kvs2 = new KV[]{
                new KV(3, 11),
                new KV(4, 12),
                new KV(5, 13),
                new KV(6, 14),
                new KV(7, 15),
        };
    }

    final static KVDecoder<KV> kvDecoder = b -> {
        assertTrue(b.values() == 1);
        return new KV(b.intKey(), b.intValue());
    };

    private static void testCursor(DBReader r, KV[] kvs) {
        for (int from = 0; from < kvs.length; from++) {
            for (int to = from; to < kvs.length; to++) {
                final KV fromKV = kvs[from];
                final KV toKV = kvs[to];
                for (int includeF = 0; includeF <= 1; includeF++) {
                    for (int includeT = 0; includeT <= 1; includeT++) {
                        for (int backward = -1; backward <= 1; backward += 2) {
                            final ArrayList<KV> rs = new ArrayList<>();
                            try (final Cursor<KV> cursor = r.from(category, includeF != 0).key(fromKV.k).to(includeT != 0).key(toKV.k).cursor(backward < 0, kvDecoder)) {
                                cursor.forEachRemaining(rs::add);
                            }
                            final int begin = from + includeF;
                            final int end = to - includeT;
                            final KV[] range = begin > end ? new KV[0] : Arrays.copyOfRange(kvs, begin, end + 1);
                            if (backward < 0) {
                                for (int i = 0, j = range.length - 1; i < j; i++, j--) {
                                    final KV k = range[i];
                                    range[i] = range[j];
                                    range[j] = k;
                                }
                            }
                            final KV[] rangeR = rs.toArray(new KV[rs.size()]);
                            assertArrayEquals(range, rangeR);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCursor() throws Throwable {
        db.runWrite(w -> {
            for (KV kv : kvs1) {
                w.write(category).key(kv.k).values(1).value(kv.v).end();
            }
            testCursor(w, kvs1);
        }, r -> {
            testCursor(r, kvs1);
        }).await();
        final NavigableMap<Integer, KV> nm = new TreeMap<>();
        for (KV kv : kvs1) {
            nm.put(kv.k, kv);
        }
        for (KV kv : kvs2) {
            nm.put(kv.k, kv);
        }
        final KV[] kvs = nm.values().toArray(new KV[nm.size()]);
        db.runWrite(w -> {
            for (KV kv : kvs2) {
                w.write(category).key(kv.k).values(1).value(kv.v).end();
            }
            testCursor(w, kvs);
        }, r -> {
            testCursor(r, kvs);
        }).await();
    }
}
