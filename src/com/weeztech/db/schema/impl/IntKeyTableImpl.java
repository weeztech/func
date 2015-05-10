package com.weeztech.db.schema.impl;

import com.weeztech.db.engine.Cursor;
import com.weeztech.db.engine.DBWriter;
import com.weeztech.db.schema.IntKeyField;
import com.weeztech.db.schema.IntKeyTable;
import com.weeztech.db.schema.impl.AbstractField.IntKeyFieldImpl;

/**
 * Created by gaojingxin on 15/4/18.
 */
final class IntKeyTableImpl extends AbstractTable implements IntKeyTable {
    final IntKeyFieldImpl keyField = new IntKeyFieldImpl("id", 0);

    IntKeyTableImpl(String name, int index) {
        super(name, index);

    }

    @Override
    public final IntKeyField keyField() {
        return keyField;
    }

    private int keySeed;

    @Override
    public final int newKey(DBWriter w) {
        if (keySeed == 0) {
            try (Cursor<Integer> c = w.fromExclude((short)index)
                    .toExclude().key(index + 1)
                    .backward((b) -> {
                        b.shortKey();//skip cid
                        return b.intKey();
                    })) {
                keySeed = c.hasNext() ? c.next() : 0;
            }
        }
        return ++keySeed;
    }
}
