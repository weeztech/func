package com.weeztech.json;

/**
 * Created by gaojingxin on 15/3/25.
 */
@FunctionalInterface
public interface JSONObjBuilder<T> {
    public void buildJSONObj(T obj, JSONObjWriter writer);
}
