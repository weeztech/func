package com.weeztech.db.engine;

/**
 * Created by gaojingxin on 15/4/2.
 */
@FunctionalInterface
public interface KVDecoder<T> {


    T decode(KVBuffer buffer);

    /**
     * 当解码不是必然返回非空对象时，请重载此方法已保证计数的准确性
     */
    default boolean test(KVBuffer buffer) {
        return true;
    }
}
