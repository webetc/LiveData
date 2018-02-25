package com.github.webetc.livedata;

import java.util.Iterator;

public interface LiveDatabase {

    void add(LiveTable table);

    void remove(LiveTable table);

    Iterator<LiveTable> getTables();

    String getPrimaryKey(String schema, String table);

    void getAllData(String schema, String table, LiveObserver observer);

    LiveResponse getInsertedSinceLast(String schema, String table);

    LiveResponse getAllWithConstraint(String schema, String table, String constraintColumn, String constraintValue);
}
