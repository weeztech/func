package com.weeztech.json;

/**
 * Created by gaojingxin on 15/3/25.
 */
public interface JSONArrayBuilder<A> {
    public void buildJSONArray(A array, JSONArrayWriter writer);
}
