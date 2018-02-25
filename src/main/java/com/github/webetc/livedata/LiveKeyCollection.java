package com.github.webetc.livedata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class LiveKeyCollection implements LiveObserver {

    private LiveDatabase database;
    private LiveTable table;
    private LiveObserver observer;
    private String keyColumn;
    private final Collection<String> keyConstraints = new HashSet<>();
    private final Collection<String> idIndex = new HashSet<>();


    public LiveKeyCollection(String schemaName, String tableName,
                             String keyColumn, Collection<String> keyConstraints,
                             LiveDatabase database, LiveObserver watcher) {
        this.database = database;
        this.table = LiveTable.get(schemaName, tableName, database);
        this.keyColumn = keyColumn.toLowerCase();
        this.keyConstraints.addAll(keyConstraints);
        this.observer = watcher;
        table.addWatcher(this);
    }


    public LiveKeyCollection(String schemaName, String tableName,
                             String keyColumn, String constraint,
                             LiveDatabase database, LiveObserver watcher) {
        this.database = database;
        this.table = LiveTable.get(schemaName, tableName, database);
        this.keyColumn = keyColumn.toLowerCase();
        this.keyConstraints.add(constraint);
        this.observer = watcher;
        table.addWatcher(this);
    }


    public void close() {
        table.removeWatcher(this);
    }


    public void addConstraint(String key) {
        synchronized (keyConstraints) {
            keyConstraints.add(key);
        }

        observer.send(database.getAllWithConstraint(table.getSchemaName(), table.getTableName(), keyColumn, key));
    }


    public void removeConstraint(String key) {
        synchronized (keyConstraints) {
            keyConstraints.remove(key);
        }

        // TODO: remove rows from existing index and notify watchers
    }


    private void updateIdIndex(String key, boolean include) {
        synchronized (idIndex) {
            if (include)
                idIndex.add(key);
            else
                idIndex.remove(key);
        }
    }


    @Override
    public void send(LiveResponse input) {
        LiveResponse response = cloneResponse(input);
        int idCol = input.getIdColumnIndex();
        Integer keyColIndex = null;

        // Find key column
        for (int i = 0; i < input.getColumns().size(); i++) {
            String rowColumn = input.getColumns().get(i);
            if (keyColumn.equals(rowColumn.toLowerCase()))
                keyColIndex = i;
        }

        // Add rows
        for (List<String> row : input.getRecords()) {
            String id = row.get(idCol);
            boolean addRow = false;

            boolean contained = false;
            synchronized (idIndex) {
                contained = idIndex.contains(id);
            }
            if (contained) {
                // Row is already in id index to just add
                addRow = true;

            } else if (keyColIndex != null) {
                // Check if row matches constraint
                String key = row.get(keyColIndex);
                boolean keyMatches = false;
                synchronized (keyConstraints) {
                    keyMatches = keyConstraints.contains(key);
                }
                if (keyMatches) {
                    // NOTE: if key column was updated to something that now matches
                    // not all of the columns will be available so will ony work
                    // on a foreign key that doesn't get changed for now.
                    updateIdIndex(id, true);
                    addRow = true;
                }
            }

            if (addRow) {
                response.addRecord(row);
                if (response.getAction().equals(LiveResponse.Delete)) {
                    updateIdIndex(id, false);
                }
            }
        }

        // Notify watchers of matching rows
        if (response.getRecords().size() > 0)
            observer.send(response);
    }


    private LiveResponse cloneResponse(LiveResponse input) {
        LiveResponse response = new LiveResponse(input.getAction(), input.getSchema(), input.getTable());
        response.setIdColumnIndex(input.getIdColumnIndex());
        response.setColumns(new ArrayList<>());
        response.getColumns().addAll(input.getColumns());
        return response;
    }
}
