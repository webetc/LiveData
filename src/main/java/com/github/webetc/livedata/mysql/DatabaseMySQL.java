package com.github.webetc.livedata.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.webetc.livedata.LiveResponse;
import com.github.webetc.livedata.LiveTable;
import com.github.webetc.livedata.LiveTransactionDatabase;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DatabaseMySQL extends LiveTransactionDatabase {

    private BinaryLogClient client;
    protected Map<String, Long> lastTableId = new HashMap<>();
    private String hostname;
    private Integer port;
    private String url;
    private String user;
    private String password;
    private String tablemap_db = null;
    private String tablemap_table = null;


    public DatabaseMySQL(String hostname, String user, String password) throws ClassNotFoundException {
        this(hostname, 3306, user, password);
    }


    public DatabaseMySQL(String hostname, Integer port, String user, String password) throws ClassNotFoundException {
        super();
        this.hostname = hostname;
        this.port = port;
        this.url = "jdbc:mysql://" + hostname + ":" + port;
        this.user = user;
        this.password = password;
        start();
    }


    public void close() throws Exception {
        super.close();
        client.disconnect();
    }


    @Override
    protected LiveResponse getData(String schema, String table, String where) {
        String action = where == null ? LiveResponse.Load : LiveResponse.Modify;
        LiveResponse response = new LiveResponse(action, schema, table);

        if (getData(response, where)) {
            String tablePath = schema.toLowerCase() + "." + table.toLowerCase();

            // Set initial last id for table
            lastTableId.putIfAbsent(tablePath, response.largestId);

            return response;
        } else {
            // Shouldn't get here unless db or network is down
            return new LiveResponse(LiveResponse.Error, schema, table);
        }
    }


    @Override
    protected LiveResponse getInserted(String schema, String table) {
        LiveResponse response = new LiveResponse(LiveResponse.Modify, schema, table);
        String tablePath = schema.toLowerCase() + "." + table.toLowerCase();
        String idCol = getPrimaryKey(schema, table);
        Long lastId = lastTableId.get(tablePath);
        if (lastId == null)
            lastId = 0L;
        String where = "where " + idCol + " > " + lastId;
        if (getData(response, where)) {
            if (response.largestId > lastId)
                lastTableId.put(tablePath, response.largestId);
            return response;
        }
        return new LiveResponse(LiveResponse.Error, schema, table);
    }


    @Override
    protected String loadPrimaryKey(String schemaName, String tableName) {
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
                List<String> row = new ArrayList<>();
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
                if (rs != null)
                    rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (con != null)
                    con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Shouldn't get here unless db or network is down
        return false;
    }


    private void start() throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");

        // Start the replication client
        client = new BinaryLogClient(hostname, port, user, password);
        client.registerEventListener(this::processDatabaseEvent);

        try {
            client.connect(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void processDatabaseEvent(Event event) {
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
                for (LiveTable l : liveTables) {
                    if (tablemap_db.equals(l.getSchemaName()) && tablemap_table.equals(l.getTableName())) {
                        // TODO: handle non-gtid mutations
                    }
                }
            }
        } else if (et == EventType.ANONYMOUS_GTID || et == EventType.GTID) {
            startTransaction();
        } else if (et == EventType.XID) {
            endTransaction(true);
        } else if (et == EventType.QUERY) {
            EventData ed = event.getData();
            if (ed != null && QueryEventData.class.isInstance(ed)) {
                QueryEventData qed = (QueryEventData) ed;
                String sql = qed.getSql().substring(0, Math.min(10, qed.getSql().length())).toLowerCase();
                if (sql.startsWith("commit")) {
                    endTransaction(true);
                } else if (sql.startsWith("rollback")) {
                    endTransaction(false);
                } else {
                    processTransactionSQL(qed.getDatabase(), qed.getSql());
                }
            }

        }
    }
}
