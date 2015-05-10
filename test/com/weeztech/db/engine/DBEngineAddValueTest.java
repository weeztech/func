package com.weeztech.db.engine;

/**
 * Created by gaojingxin on 15/4/30.
 */

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;

/**
 * Created by gaojingxin on 15/4/27.
 */
@RunWith(Parameterized.class)
public class DBEngineAddValueTest {
    static final short cat = 1;
    private final long[] values;
    private final Object[] keys;

    public DBEngineAddValueTest(Object[] keys, long[] values) {
        this.values = values;
        this.keys = keys;
    }

    @Parameterized.Parameters
    public static Object[][] values() {
        return new Object[][]{{
                new Object[]{
                        1
                },
                new long[]{
                        1, 2, 3
                }
        }, {
                new Object[]{
                        2
                },
                new long[]{
                        -1, -2, -3
                }
        }};
    }

    @ClassRule
    public static final DBEngineResource db = new DBEngineResource();

    public static long[] mult(long[] values, int m) {
        if (m == 1) {
            return values;
        }
        final long[] r = values.clone();
        for (int i = 0; i < r.length; i++) {
            r[i] *= m;
        }
        return r;
    }

    @Test
    public void testAdd() throws Throwable {
        final long[] v2 = mult(values, 2);
        final long[] v3 = mult(values, 3);
        final long[] v4 = mult(values, 4);
        db.runWrite(w -> {
            w.addSums(cat, keys, values);
            w.addSums(cat, keys, values);
            w.addSums(cat, keys, values);
            w.get(cat, keys, b -> {
                assertTrue(b.sumsEQ(v3));
                return null;
            });
        }, r -> {
            r.get(cat, keys, b -> {
                assertTrue(b.sumsEQ(v3));
                return null;
            });
        }).await();
        db.runWrite(w -> {
            w.addSums(cat, keys, values);
            w.get(cat, keys, b -> {
                assertTrue(b.sumsEQ(v4));
                return null;
            });
        }, r -> {
            r.get(cat, keys, b -> {
                assertTrue(b.sumsEQ(v4));
                return null;
            });
        }).await();
        db.runWrite(w -> {
            w.putSums(cat, keys, values);
            w.get(cat, keys, b -> {
                assertTrue(b.sumsEQ(values));
                return null;
            });
            w.addSums(cat, keys, values);
        }, r -> {
            r.get(cat, keys, b -> {
                assertTrue(b.sumsEQ(v2));
                return null;
            });
        }).await();
        db.runWrite(w -> {
            w.putSums(cat, keys, values);
            w.addSums(cat, keys, values);
            w.get(cat, keys, b -> {
                assertTrue(b.sumsEQ(v2));
                return null;
            });
            w.putSums(cat, keys, values);
        }, r -> {
            r.get(cat, keys, b -> {
                assertTrue(b.sumsEQ(values));
                return null;
            });
        }).await();
    }
}
