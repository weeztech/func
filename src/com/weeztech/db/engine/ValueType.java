package com.weeztech.db.engine;

/**
 * Created by gaojingxin on 15/4/28.
 */
public enum ValueType {
    NULL,
    ZERO,
    ONE,
    BYTE,
    SHORT,
    INT,
    INT48,
    LONG,
    STRING,
    TUPLE;
    public final static long MIN_INT_48 = 0xFFFF_8000_0000_0000L;
    public final static long MAX_INT_48 = 0x0000_7FFF_FFFF_FFFFL;
}
