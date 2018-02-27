package com.github.webetc.livedata;

import com.github.webetc.livedata.mysql.DatabaseMySQL;
import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.config.SchemaConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.ScriptResolver.classPathScript;
import static com.wix.mysql.config.Charset.UTF8;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.config.SchemaConfig.aSchemaConfig;
import static com.wix.mysql.distribution.Version.v5_7_latest;


public class DatabaseConfig {

    private static EmbeddedMysql mysqld;
    private static SchemaConfig schema;
    private static String url;
    private static String user = "root";
    private static String password = "";
    private static int port = 4306;
    private static String schemaName = "example";
    private static LiveDatabase database = null;


    public static LiveDatabase getDatabase() throws Exception {
        if (url == null) {
            setup();
        }

        return database;
    }


    public static String getSchemaName() {
        return schemaName;
    }


    public static void reloadSchema() throws Exception {
        if (url == null)
            setup();
        mysqld.reloadSchema(schema);
    }


    public static Connection getConnection() throws Exception {
        if (url == null)
            setup();
        return DriverManager.getConnection(url, user, password);
    }


    public static void watchTable(String table) throws Exception {
        if (url == null)
            setup();
        LiveTable.get(schemaName, table, database);
    }


    public static void addWatcher(String table, LiveObserver watcher) throws Exception {
        if (url == null)
            setup();
        LiveTable liveTable = LiveTable.get(schemaName, table, database);
        liveTable.addWatcher(watcher);
    }


    public static void removeWatcher(String table, LiveObserver watcher) throws Exception {
        if (mysqld == null)
            setup();
        LiveTable liveTable = LiveTable.get(schemaName, table, database);
        liveTable.removeWatcher(watcher);
    }


    private static synchronized void setup() throws Exception {
        if (url == null) {
            url = "jdbc:mysql://localhost:" + port + "/" + schemaName;

            MysqldConfig config = aMysqldConfig(v5_7_latest)
                    .withCharset(UTF8)
                    .withPort(4306)
                    .withServerVariable("log-bin", "embedded_bin.log")
                    .withServerVariable("log-bin-index", "embedded_bin.index")
                    .withServerVariable("max-binlog-size", "10M")
                    .withServerVariable("binlog-format", "MIXED")
                    .withServerVariable("server-id", "1999")
                    .build();

            schema = aSchemaConfig(schemaName)
                    .withScripts(classPathScript("example_init.sql"))
                    .withCharset(UTF8)
                    .build();

            mysqld = anEmbeddedMysql(config)
                    .addSchema(schema)
                    .start();

            database = new DatabaseMySQL("localhost", 4306, "root", "");
        }
    }


    public static List<Map<String, String>> convertResponse(LiveResponse response) {
        List<Map<String, String>> rows = new ArrayList<>();
        for (List<String> row : response.getRecords()) {
            Map<String, String> m = new HashMap<>();
            for (int i = 0; i < response.getColumns().size(); i++) {
                m.put(response.getColumns().get(i), row.get(i).toString());
            }
            rows.add(m);
        }
        return rows;
    }


    public static List<Map<String, String>> query(String query) throws Exception {
        Connection con = null;
        ResultSet rs = null;
        List<Map<String, String>> results = new ArrayList<>();

        try {
            con = getConnection();
            Statement stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                Map<String, String> row = new HashMap<>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
                    row.put(rs.getMetaData().getColumnName(i), rs.getObject(i).toString());
                results.add(row);
            }

            return results;

        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void execute(String query) throws Exception {
        Connection con = null;
        List<Map<String, String>> results = new ArrayList<>();

        try {
            con = getConnection();
            Statement stmt = con.createStatement();
            stmt.execute(query);

        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
