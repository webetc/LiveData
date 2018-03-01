package com.github.webetc.livedata;

import java.util.*;

public class LiveKeyCollection implements LiveObserver {

    protected LiveDatabase database;
    protected LiveTable table;
    protected LiveObserver observer;
    private String keyColumn;
    private final Collection<String> keyConstraints = new HashSet<>();
    private final Map<String, String> idKeyIndex = new HashMap<>();
    private final Map<String, Collection<String>> keyIdIndex = new HashMap<>();


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

    public LiveKeyCollection(String schemaName, String tableName,
                             String keyColumn,
                             LiveDatabase database, LiveObserver watcher) {
        this.database = database;
        this.table = LiveTable.get(schemaName, tableName, database);
        this.keyColumn = keyColumn.toLowerCase();
        this.observer = watcher;
        table.addWatcher(this, false);
    }


    public void close() {
        table.removeWatcher(this);
    }


    public void addConstraint(String key) {
        synchronized (keyConstraints) {
            keyConstraints.add(key);
        }

        database.add(LiveDatabase.LiveEvent.create(
                table.getSchemaName(),
                table.getTableName(),
                "where " + keyColumn + " = '" + key + "'",
                this));
    }


    public void addConstraints(Collection<String> keys) {
        if (keys == null || keys.size() == 0)
            return;

        if (keys.size() == 1) {
            addConstraint(keys.iterator().next());
            return;
        }

        synchronized (keyConstraints) {
            keyConstraints.addAll(keys);
        }

        StringBuilder where = new StringBuilder("where " + keyColumn + " in (");
        boolean first = true;
        for (String key : keys) {
            if (first) {
                first = false;
                where
                        .append("'")
                        .append(key)
                        .append("'");
            } else {
                where
                        .append(", '")
                        .append(key)
                        .append("'");
            }
        }
        where.append(")");

        database.add(LiveDatabase.LiveEvent.create(
                table.getSchemaName(),
                table.getTableName(),
                where.toString(),
                this));
    }


    public void removeConstraint(String key) {
        synchronized (keyConstraints) {
            keyConstraints.remove(key);
        }

        // Remove rows from existing indexes
        Collection<String> keyIds;
        synchronized (keyIdIndex) {
            keyIds = keyIdIndex.get(key);
            keyIdIndex.remove(key);
        }
        synchronized (idKeyIndex) {
            if (keyIds != null) {
                for (String id : keyIds) {
                    idKeyIndex.remove(id);
                }
            }
        }

        if (keyIds != null && keyIds.size() > 0)
            notifyIdRemoval(keyIds);
    }


    public void removeConstraints(Collection<String> keys) {
        synchronized (keyConstraints) {
            for (String key : keys)
                keyConstraints.remove(key);
        }

        // Remove rows from existing indexes
        Collection<String> keyIds = new ArrayList<>();
        synchronized (keyIdIndex) {
            for (String key : keys) {
                keyIds.addAll(keyIdIndex.get(key));
                keyIdIndex.remove(key);
            }
        }
        synchronized (idKeyIndex) {
            for (String id : keyIds) {
                idKeyIndex.remove(id);
            }
        }

        if (keyIds.size() > 0)
            notifyIdRemoval(keyIds);
    }


    private void updateIdIndex(String id, String key) {
        if (key != null) {
            synchronized (idKeyIndex) {
                idKeyIndex.put(id, key);
            }
            synchronized (keyIdIndex) {
                Collection<String> ids = keyIdIndex.get(key);
                if (ids == null) {
                    ids = new HashSet<>();
                    keyIdIndex.put(key, ids);
                }
                ids.add(id);
            }
        } else {
            synchronized (idKeyIndex) {
                key = idKeyIndex.get(id);
                idKeyIndex.remove(id);
            }
            synchronized (keyIdIndex) {
                Collection<String> ids = keyIdIndex.get(key);
                if (ids != null)
                    ids.remove(id);
            }
        }
    }


    @Override
    public void send(LiveResponse input) {
        LiveResponse response = cloneResponse(input);
        int idCol = input.getIdColumnIndex();
        Integer keyColIndex = null;
        Collection<String> addedIds = new ArrayList<>();
        Collection<String> removedIds = new ArrayList<>();

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

            boolean contained;
            synchronized (idKeyIndex) {
                contained = idKeyIndex.get(id) != null;
            }
            if (contained) {
                // Row is already in id index to just add
                addRow = true;

            } else if (keyColIndex != null) {
                // Check if row matches constraint
                String key = row.get(keyColIndex);
                boolean keyMatches;
                synchronized (keyConstraints) {
                    keyMatches = keyConstraints.contains(key);
                }
                if (keyMatches) {
                    /*
                    NOTE: if key column was updated to something that now matches
                    not all of the columns will be available so will ony work
                    on a foreign key that doesn't get changed for now.
                    */
                    updateIdIndex(id, key);
                    addRow = true;
                    addedIds.add(id);
                }
            }

            if (addRow) {
                response.addRecord(row);
                if (response.getAction().equals(LiveResponse.Delete)) {
                    updateIdIndex(id, null);
                    removedIds.add(id);
                }
            }
        }

        // Notify watcher of matching rows
        if (response.getRecords() != null && response.getRecords().size() > 0)
            observer.send(response);

        if (addedIds.size() > 0)
            this.addedIds(addedIds);
        if (removedIds.size() > 0)
            this.removedIds(removedIds);
    }


    protected void addedIds(Collection<String> ids) {

    }


    protected void removedIds(Collection<String> ids) {

    }


    private void notifyIdRemoval(Collection<String> ids) {
        removedIds(ids);

        // Not removed from db but need removed from observer
        LiveResponse response = new LiveResponse(LiveResponse.Delete, table.getSchemaName(), table.getTableName());
        response.addColumn(database.getPrimaryKey(table.getSchemaName(), table.getTableName()));
        for (String id : ids) {
            List<String> row = new ArrayList<>();
            row.add(id);
            response.addRecord(row);
        }

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
