package com.weeztech.db.schema;

/**
 * Created by gaojingxin on 15/4/17.
 */
public interface SeqValueField extends Field {
    int fieldCount();

    Field getField(int index);

    default Field findField(String name) {
        for (int i = fieldCount() - 1; i >= 0; i--) {
            Field f = getField(i);
            if (f.name().equals(name)) {
                return f;
            }
        }
        return null;
    }
}
