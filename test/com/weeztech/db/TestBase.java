package com.weeztech.db;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by gaojingxin on 15/4/28.
 */
public class TestBase {
    protected static boolean randomBool() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    protected static byte randomByte() {
        return (byte) ThreadLocalRandom.current().nextInt();
    }

    protected static short randomShort() {
        return (short) ThreadLocalRandom.current().nextInt();
    }

    protected static char randomChar() {
        return (char) ThreadLocalRandom.current().nextInt(1, Character.MAX_VALUE);
    }

    protected static int randomInt() {
        return (byte) ThreadLocalRandom.current().nextInt();
    }

    protected static int randomInt(int origin, int bound) {
        return (byte) ThreadLocalRandom.current().nextInt(origin, bound);
    }

    protected static String randomString(int length) {
        final char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = randomChar();
        }
        return new String(chars);
    }

    protected static String randomString(int minLength, int maxLength) {
        return randomString(randomInt(minLength, maxLength + 1));
    }

    protected static long randomLong() {
        return ThreadLocalRandom.current().nextLong();
    }
}
