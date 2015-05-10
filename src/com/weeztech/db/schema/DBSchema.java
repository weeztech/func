package com.weeztech.db.schema;

import com.weeztech.AbstractServer;

/**
 * Created by gaojingxin on 15/4/17.
 */
public interface DBSchema extends AbstractServer {
    int tableCount();

    Table getTable(int index);

    Table findTable(String name);
}
