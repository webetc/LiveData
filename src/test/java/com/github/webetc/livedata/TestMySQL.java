package com.github.webetc.livedata;

import com.google.gson.Gson;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


public class TestMySQL {

    private static Gson gson = new Gson();
    private static Watcher watcher = new Watcher();


    @BeforeClass
    public static void beforeClass() throws Exception {
        DatabaseConfig.getDatabase();
    }


    @Before
    public void before() throws Exception {
        DatabaseConfig.reloadSchema();
        watcher.reset();
        DatabaseConfig.addWatcher("person", watcher);
        LiveResponse response = watcher.getLast();
        assertEquals(LiveResponse.Load, response.getAction());
        assertEquals("person", response.getTable());
    }


    @After
    public void after() throws Exception {
        DatabaseConfig.removeWatcher("person", watcher);
    }


    @org.junit.Test
    public void test_db_setup() throws Exception {
        List<Map<String, String>> results = DatabaseConfig.query("select * from person where name = 'Bob'");
        assertEquals("Results size", 1, results.size());
        assertNotNull("Bob id exists", results.get(0).get("id"));
    }


    @org.junit.Test
    public void test_bin_log() throws Exception {
        List<Map<String, String>> results = DatabaseConfig.query("SHOW BINARY LOGS");
        assertTrue("Results size", results.size() > 0);
    }


    @org.junit.Test
    public void test_insert() throws Exception {
        DatabaseConfig.execute("insert into person(name) values('Doug')");
        LiveResponse response = watcher.getLast();
        assertEquals(LiveResponse.Modify, response.getAction());
        assertEquals("person", response.getTable());
        assertEquals(1, response.getRecords().size());
        String name = DatabaseConfig.convertResponse(response).get(0).get("name");
        assertEquals("Doug", name);
    }
}
