package com.weeztech.db.schema.impl;

import com.weeztech.db.schema.Field;
import com.weeztech.db.schema.Table;

/**
 * Created by gaojingxin on 15/4/18.
 */
public abstract class AbstractTable implements Table {
    final String name;
    final int index;

    AbstractTable(String name, int index) {
        this.name = name;
        this.index = index;
    }

    static final AbstractField[] EMPTY_FIELDS = new AbstractField[0];

    private AbstractField[] valueFields = EMPTY_FIELDS;

    protected final void setValueFields(AbstractField[] valueFields) {
        this.valueFields = valueFields;
    }

    @Override
    public final String name() {
        return name;
    }

    @Override
    public final int valueFieldCount() {
        return valueFields.length;
    }

    @Override
    public final AbstractField getValueField(int index) {
        return valueFields[index];
    }

    @Override
    public final int index() {
        return index;
    }

    final static class NullTable extends AbstractTable {
        @Override
        public int keyFieldCount() {
            return 0;
        }

        @Override
        public Field getKeyField(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Field findKeyField(String name) {
            return null;
        }

        NullTable(int index) {
            super("", index);
        }
    }
}
