package com.weeztech.db.schema;

/**
 * Created by gaojingxin on 15/4/17.
 */
public interface RefValueField extends Field {
    /**
     * 引用的表的Index
     */
    int refTableIndex();
}
