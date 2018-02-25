package com.github.webetc.livedata.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.webetc.livedata.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class DatabaseMySQL implements LiveDatabase {

    private List<LiveTable> liveTables = new CopyOnWriteArrayList<>();
    private Map<String, String> primaryKeys = new HashMap<>();
    private Map<String, Long> lastTableId = new HashMap<String, Long>();
    private LiveTransaction transaction;
    private String hostname;
    private Integer port = 3306;
    private String url;
    private String user;
    private String password;


    public DatabaseMySQL(String hostname, String user, String password) {
        this.hostname = hostname;
        this.url = "jdbc:mysql://" + hostname;
        this.user = user;
        this.password = password;
        this.transaction = new LiveTransaction(this);
    }


    public DatabaseMySQL(String hostname, Integer port, String user, String password) {
        this.hostname = hostname;
        this.port = port;
        this.url = this.url = "jdbc:mysql://" + hostname + ":" + port;
        this.user = user;
        this.password = password;
        this.transaction = new LiveTransaction(this);
    }


    public void add(LiveTable table) {
        liveTables.add(table);
    }


    public void remove(LiveTable table) {
        liveTables.remove(table);
    }


    @Override
    public Iterator<LiveTable> getTables() {
        return liveTables.iterator();
    }


    public void getAllData(String schema, String table, LiveObserver observer) {
        LiveResponse response = new LiveResponse(LiveResponse.Load, schema, table);

        if (getData(response, null)) {
            String tablePath = schema.toLowerCase() + "." + table.toLowerCase();

            // Set initial last id for table
            if (lastTableId.get(tablePath) == null) {
                lastTableId.put(tablePath, response.largestId);
            }

            // Send response
            observer.send(response);
        } else {
            // Shouldn't get here unless db or network is down
            observer.send(new LiveResponse(LiveResponse.Error, schema, table));
        }
    }


    @Override
    public LiveResponse getInsertedSinceLast(String schema, String table) {
        LiveResponse response = new LiveResponse(LiveResponse.Modify, schema, table);
        String tablePath = schema.toLowerCase() + "." + table.toLowerCase();
        String idCol = getPrimaryKey(schema, table);
        String where = "where " + idCol + " > " + lastTableId.get(tablePath);
        if (getData(response, where)) {
            lastTableId.put(tablePath, response.largestId);
            return response;
        }
        return new LiveResponse(LiveResponse.Error, schema, table);
    }


    @Override
    public LiveResponse getAllWithConstraint(String schema, String table, String constraintColumn, String constraintValue) {
        LiveResponse response = new LiveResponse(LiveResponse.Modify, schema, table);
        String where = "where " + constraintColumn + " = " + constraintValue;
        if (getData(response, where)) {
            return response;
        }
        return new LiveResponse(LiveResponse.Error, schema, table);
    }


    private boolean getData(LiveResponse response, String where) {
        Connection con = null;
        ResultSet rs = null;
        String schema = response.getSchema();
        String table = response.getTable();

        try {
            String idCol = getPrimaryKey(schema, table);
            if (idCol == null)
                throw new Exception("Primary key not found for " + schema + "." + table);

            con = DriverManager.getConnection(url, user, password);
            Statement stmt = con.createStatement();
            if (where != null)
                rs = stmt.executeQuery("select * from " + schema + "." + table + " " + where);
            else
                rs = stmt.executeQuery("select * from " + schema + "." + table);
            ResultSetMetaData rsmd = rs.getMetaData();
            int idColIndex = 0;

            // Set columns
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String columnName = rsmd.getColumnName(i);
                response.addColumn(columnName);
                if (columnName.toLowerCase().equals(idCol)) {
                    idColIndex = i;
                    response.setIdColumnIndex(i - 1);
                }
            }

            // Add row data
            while (rs.next()) {
                List<String> row = new ArrayList<String>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    Object o = rs.getObject(i);
                    if (o != null)
                        row.add(o.toString());  // TODO: format by data type
                    else
                        row.add(null);

                    // Find last id for table
                    if (i == idColIndex && Long.class.isInstance(o)) {
                        long idVal = (Long) o;
                        if (idVal > response.largestId)
                            response.largestId = idVal;
                    } else if (i == idColIndex && Integer.class.isInstance(o)) {
                        int idVal = (Integer) o;
                        if (idVal > response.largestId)
                            response.largestId = idVal;
                    }
                }
                response.addRecord(row);
            }

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Shouldn't get here unless db or network is down
        return false;
    }


    public void start() throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        BinaryLogClient client = new BinaryLogClient(hostname, port, user, password);
        client.registerEventListener(new BinaryLogClient.EventListener() {

            String tablemap_db = null;
            String tablemap_table = null;

            @Override
            public void onEvent(Event event) {
                EventType et = event.getHeader().getEventType();

                if (et == EventType.TABLE_MAP) {
                    EventData ed = event.getData();
                    if (ed != null && TableMapEventData.class.isInstance(ed)) {
                        TableMapEventData tmed = (TableMapEventData) ed;
                        tablemap_db = tmed.getDatabase().toLowerCase();
                        tablemap_table = tmed.getTable().toLowerCase();
                    } else {
                        tablemap_db = null;
                        tablemap_table = null;
                    }
                } else if (EventType.isRowMutation(et)) {
                    if (tablemap_db != null && tablemap_table != null) {
                        Iterator<LiveTable> iLiveTable = liveTables.iterator();
                        while (iLiveTable.hasNext()) {
                            LiveTable l = iLiveTable.next();
                            if (tablemap_db.equals(l.getSchemaName()) && tablemap_table.equals(l.getTableName())) {
                                // TODO: handle non-gtid mutations
                            }
                        }
                    }
                } else if (et == EventType.ANONYMOUS_GTID || et == EventType.GTID) {
                    transaction.start();
                } else if (et == EventType.XID) {
                    transaction.end(true);
                } else if (et == EventType.QUERY) {
                    EventData ed = event.getData();
                    if (ed != null && QueryEventData.class.isInstance(ed)) {
                        QueryEventData qed = (QueryEventData) ed;
                        String sql = qed.getSql().substring(0, Math.min(10, qed.getSql().length())).toLowerCase();
                        if (sql.startsWith("commit")) {
                            transaction.end(true);
                        } else if (sql.startsWith("rollback")) {
                            transaction.end(false);
                        } else {
                            transaction.process(qed.getDatabase(), qed.getSql());
                        }
                    }

                } else {
//                    System.err.println("\n" + event.toString());
                }
            }
        });

        try {
            client.connect(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
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


    private String loadPrimaryKey(String schemaName, String tableName) {
        Connection con = null;
        ResultSet keys = null;
        String primaryKey = null;
        int count = 0;

        try {
            con = DriverManager.getConnection(url, user, password);
            DatabaseMetaData meta = con.getMetaData();
            keys = meta.getPrimaryKeys(schemaName, null, tableName);
            while (keys.next()) {
                count++;
                primaryKey = keys.getString("COLUMN_NAME");
            }

            if (count == 1)
                return primaryKey;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (keys != null)
                    keys.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (con != null)
                    con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Currently don't handle multiple keys
        return null;
    }

}
