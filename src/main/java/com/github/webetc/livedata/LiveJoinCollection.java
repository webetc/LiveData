package com.github.webetc.livedata;

import java.util.Collection;

public class LiveJoinCollection extends LiveKeyCollection {

    private LiveJoinCollection joinCollection = null;


    public LiveJoinCollection(String schemaName, String tableName,
                              String keyColumn, String constraint,
                              LiveDatabase database, LiveObserver watcher) {
        super(schemaName, tableName, keyColumn, constraint, database, watcher);
    }


    public LiveJoinCollection(String schemaName, String tableName,
                              String keyColumn,
                              LiveDatabase database, LiveObserver watcher) {
        super(schemaName, tableName, keyColumn, database, watcher);
    }


    public void close() {
        if (joinCollection != null)
            joinCollection.close();
        super.close();
    }


    public LiveJoinCollection join(String schemaName, String tableName, String foreignKeyColumn) {

        joinCollection = new LiveJoinCollection(schemaName, tableName,
                foreignKeyColumn,
                this.database, this.observer);
        return joinCollection;
    }

    public LiveJoinCollection join(String tableName, String foreignKeyColumn) {
        return join(table.getSchemaName(), tableName, foreignKeyColumn);
    }


    @Override
    protected void addedIds(Collection<String> ids) {
        super.addedIds(ids);
        if (joinCollection != null)
            joinCollection.addConstraints(ids);
    }


    @Override
    protected void removedIds(Collection<String> ids) {
        super.removedIds(ids);
        if (joinCollection != null)
            joinCollection.removeConstraints(ids);
    }
}
