package com.weeztech.db.engine;

import com.weeztech.db.TestBase;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;

/**
 * Created by gaojingxin on 15/4/27.
 */
@RunWith(Parameterized.class)
public class DBEngineWriteValueTest extends TestBase {

    private final short cat;
    private final Object[] keys;
    private final Object[] values;

    public DBEngineWriteValueTest(short cat, Object[] keys, Object[] values) {
        this.cat = cat;
        this.keys = keys;
        this.values = values;
    }

    @Parameterized.Parameters
    public static Object[][] params() {
        return new Object[][]{{
                (short)1,
                new Object[]{
                        true,
                        0,
                        1,
                        Byte.MAX_VALUE,
                        Short.MAX_VALUE,
                        Integer.MAX_VALUE,
                        Long.MAX_VALUE,
                        "abc",
                        "高京欣"
                },
                new Object[]{
                        null,
                        false,
                        true,
                        0,
                        1,
                        Byte.MIN_VALUE,
                        Byte.MAX_VALUE,
                        Short.MIN_VALUE,
                        Short.MAX_VALUE,
                        Integer.MIN_VALUE,
                        Integer.MAX_VALUE,
                        ValueType.MIN_INT_48,
                        ValueType.MAX_INT_48,
                        Long.MIN_VALUE,
                        Long.MAX_VALUE,
                        "abcdefghijklmn",
                        "123123123123123123123123123123123高高高高高高高高高高高高高高",
                        new Object[]{//tuple
                                null,
                                false,
                                true,
                                0,
                                1,
                                Byte.MIN_VALUE,
                                Byte.MAX_VALUE,
                                Short.MIN_VALUE,
                                Short.MAX_VALUE,
                                Integer.MIN_VALUE,
                                Integer.MAX_VALUE,
                                ValueType.MIN_INT_48,
                                ValueType.MAX_INT_48,
                                Long.MIN_VALUE,
                                Long.MAX_VALUE,
                                "abcdefghijklmn",
                                "123123123123123123123123123123123高高高高高高高高高高高高高高",
                        }
                }
        },{
                (short)2,
                new Object[]{
                        1
                },
                new Object[]{
                        null,null,null,null,
                        1,
                        null,null,null,null,null,
                        2,
                        null,null,null,null,null,null,
                        3
                }
        }};
    }

    @ClassRule
    public static final DBEngineResource db = new DBEngineResource();

    @Test
    public void insertTest() throws Throwable {
        db.runWrite(w -> {
            w.delete(cat,keys);
            w.write(cat, keys, values);
            w.get(cat, keys, b -> {
                assertTrue("in trans insert error", b.valuesEQ(values));
                return null;
            });
        }, r -> {
            r.get(cat, keys, b -> {
                assertTrue("after trans insert error", b.valuesEQ(values));
                return null;
            });
        }).await();
    }
}
