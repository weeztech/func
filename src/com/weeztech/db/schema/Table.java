package com.weeztech.db.schema;

/**
 * Created by gaojingxin on 15/4/17.
 */
public interface Table extends Field {
    int keyFieldCount();

    Field getKeyField(int index);

    Field findKeyField(String name);

    int valueFieldCount();

    Field getValueField(int index);
}
