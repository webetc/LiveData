package com.github.webetc.livedata;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class LiveDatabase {

    public static class LiveEvent {
        public static LiveEvent create(String schema, String table, LiveObserver observer) {
            return new LiveEventRequest(schema, table, null, observer);
        }

        public static LiveEvent create(String schema, String table, String where, LiveObserver observer) {
            return new LiveEventRequest(schema, table, where, observer);
        }

        public static LiveEvent create(Collection<LiveResponse> responses) {
            return new LiveEventResponse(responses);
        }
    }

    private static class LiveEventRequest extends LiveEvent {

        String schema;
        String table;
        String where;
        LiveObserver observer;

        LiveEventRequest(String schema, String table, String where, LiveObserver observer) {
            this.schema = schema;
            this.table = table;
            this.where = where;
            this.observer = observer;
        }
    }

    private static class LiveEventResponse extends LiveEvent {

        Collection<LiveResponse> responses;

        LiveEventResponse(Collection<LiveResponse> responses) {
            this.responses = responses;
        }
    }


    private boolean running = true;
    protected List<LiveTable> liveTables = new CopyOnWriteArrayList<>();
    private BlockingQueue<LiveEvent> operationQueue = new LinkedBlockingDeque<>();
    private Map<String, String> primaryKeys = new HashMap<>();


    public LiveDatabase() {
        // LiveEvent queue processing thread
        Thread thread = new Thread(() -> {
            while (running) {
                try {
                    LiveEvent event = operationQueue.take();
                    if (LiveEventRequest.class.isInstance(event)) {
                        // Send data so specific observer
                        sendData((LiveEventRequest) event);
                    } else if (LiveEventResponse.class.isInstance(event)) {
                        LiveEventResponse ler = (LiveEventResponse) event;
                        // Notify LiveTable watchers
                        for (LiveResponse response : ler.responses) {
                            Iterator<LiveTable> iLiveTable = getTables();
                            String lowerSchema = response.getSchema().toLowerCase();
                            String lowerTable = response.getTable().toLowerCase();
                            while (iLiveTable.hasNext()) {
                                LiveTable l = iLiveTable.next();
                                if (lowerTable.equals(l.getTableName())
                                        && lowerSchema.equals(l.getSchemaName())) {
                                    l.notifyWatchers(response);
                                }
                            }
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });

        thread.start();
    }


    public void close() throws Exception {
        running = false;
    }


    public void add(LiveEvent event) {
        operationQueue.add(event);
    }


    public void add(LiveTable table) {
        liveTables.add(table);
    }


    public Iterator<LiveTable> getTables() {
        return liveTables.iterator();
    }


    public String getPrimaryKey(String schemaName, String tableName) {
        String tablePath = schemaName.toLowerCase() + "." + tableName.toLowerCase();
        String key = primaryKeys.get(tablePath);
        if (key == null) {
            key = loadPrimaryKey(schemaName, tableName);
            primaryKeys.put(tablePath, key);
        }
        return key;
    }


    private void sendData(LiveEventRequest ler) {
        ler.observer.send(getData(ler.schema, ler.table, ler.where));
    }


    protected abstract String loadPrimaryKey(String schemaName, String tableName);


    protected abstract LiveResponse getData(String schema, String table, String where);


    protected abstract LiveResponse getInserted(String schema, String table);

}
