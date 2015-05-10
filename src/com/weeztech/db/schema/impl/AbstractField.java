package com.weeztech.db.schema.impl;

import com.weeztech.db.schema.*;

/**
 * Created by gaojingxin on 15/4/18.
 */
abstract class AbstractField implements Field {
    final String name;
    final int index;

    AbstractField(String name, int index) {
        this.name = name;
        this.index = index;
    }

    @Override
    public final String name() {
        return name;
    }

    @Override
    public final int index() {
        return index;
    }

    final static class BoolKeyFieldImpl extends AbstractField implements BoolKeyField {
        BoolKeyFieldImpl(String name, int index) {
            super(name, index);
        }
    }

    final static class IntKeyFieldImpl extends AbstractField implements IntKeyField {
        IntKeyFieldImpl(String name, int index) {
            super(name, index);
        }
    }

    final static class LongKeyFieldImpl extends AbstractField implements LongKeyField {
        LongKeyFieldImpl(String name, int index) {
            super(name, index);
        }
    }

    final static class StringKeyFieldImpl extends AbstractField implements StringKeyField {
        StringKeyFieldImpl(String name, int index) {
            super(name, index);
        }
    }
}
