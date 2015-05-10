package com.weeztech.db.schema;

import com.weeztech.db.engine.DBWriter;

/**
 * Created by gaojingxin on 15/4/18.
 */
public interface IntKeyTable extends Table {
    IntKeyField keyField();

    int newKey(DBWriter w);

    @Override
    default int keyFieldCount() {
        return 1;
    }

    @Override
    default IntKeyField getKeyField(int index) {
        if (index == 0) {
            return keyField();
        }
        throw new IllegalArgumentException("index");
    }

    @Override
    default IntKeyField findKeyField(String name) {
        final IntKeyField kf = keyField();
        return kf.name().equals(name) ? kf : null;
    }
}
