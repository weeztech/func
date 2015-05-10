package com.weeztech.utils;

import sun.misc.Unsafe;

/**
 * Created by gaojingxin on 15/3/30.
 */
public class Unsf {
    public final static sun.misc.Unsafe theUnsafe;

    public static RuntimeException throwException(Throwable e) {
        Unsf.theUnsafe.throwException(e);
        return new RuntimeException(e);
    }

    private final static long stringCharsOffset;

    static {
        Unsafe unsf;
        try {
            final java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsf = (sun.misc.Unsafe) f.get(null);
        } catch (Throwable e) {
            unsf = null;
        }
        theUnsafe = unsf;
        long sco;
        try {
            sco = unsf.objectFieldOffset(String.class.getDeclaredField("value"));
        } catch (Throwable e) {
            sco = Unsafe.INVALID_FIELD_OFFSET;
        }
        stringCharsOffset = sco;
    }


    public static char[] getStringChars(String s) {
        return (char[]) theUnsafe.getObject(s, stringCharsOffset);
    }

    public static long allocateMemory(int size) {
        return theUnsafe.allocateMemory(size);
    }

    public static long cloneMemory(long ptr, int size) {
        if (size == 0) {
            return 0;
        }
        final long newPtr = theUnsafe.allocateMemory(size);
        theUnsafe.copyMemory(ptr, newPtr, size);
        return newPtr;
    }

    public static void freeMemory(long address) {
        theUnsafe.freeMemory(address);
    }

    public static void freeMemoryByPtr(long address) {
        theUnsafe.freeMemory(theUnsafe.getAddress(address));
    }

    public static void freeMemoryByPtr(long address, int offset, int ptrOffset) {
        theUnsafe.freeMemory(theUnsafe.getAddress(address + offset * 8) + ptrOffset);
    }

    public static long getAddress(long address, int index) {
        return theUnsafe.getAddress(address + index * 8);
    }

    public static void putAddress(long address, int index, long put) {
        theUnsafe.putAddress(address + index * 8, put);
    }

}
