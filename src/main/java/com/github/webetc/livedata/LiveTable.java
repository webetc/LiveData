package com.github.webetc.livedata;

import java.util.Iterator;

public class LiveTable extends LiveObservable {

    private LiveDatabase database;
    private String schemaName = null;
    private String tableName = null;


    public static synchronized LiveTable get(String schemaName, String tableName, LiveDatabase database) {
        Iterator<LiveTable> iLiveTable = database.getTables();
        while (iLiveTable.hasNext()) {
            LiveTable table = iLiveTable.next();
            if (table.getSchemaName().equals(schemaName.toLowerCase())
                    && table.getTableName().equals(tableName.toLowerCase())) {
                return table;
            }
        }

        return new LiveTable(schemaName, tableName, database);
    }


    private LiveTable(String schemaName, String tableName, LiveDatabase database) {
        this.database = database;
        this.schemaName = schemaName.toLowerCase();
        this.tableName = tableName.toLowerCase();
        database.add(this);
    }


    @Override
    public void addWatcher(LiveObserver o) {
        super.addWatcher(o);
        database.getAllData(this.schemaName, this.tableName, o);
    }


    public String getSchemaName() {
        return schemaName;
    }


    public String getTableName() {
        return tableName;
    }
}
