package com.github.webetc.livedata;

import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class TestLiveTable {

    private static Watcher watcher = new Watcher();
    private LiveResponse response;
    private List<Map<String, String>> results;


    @Before
    public void before() throws Exception {
        DatabaseConfig.reloadSchema();
        watcher.reset();
        DatabaseConfig.addWatcher("person", watcher);
        response = watcher.getLast();
        assertEquals(LiveResponse.Load, response.getAction());
        assertEquals("person", response.getTable());
    }


    @After
    public void after() throws Exception {
        DatabaseConfig.removeWatcher("person", watcher);
    }


    @org.junit.Test
    public void test_insert() throws Exception {
        DatabaseConfig.execute("insert into person(name) values('Doug')");
        response = watcher.getLast();
        assertEquals(LiveResponse.Modify, response.getAction());
        assertEquals("person", response.getTable());
        assertEquals(1, response.getRecords().size());
        results = DatabaseConfig.convertResponse(response);
        assertEquals("Doug", results.get(0).get("name"));
    }


    @org.junit.Test
    public void test_update() throws Exception {
        DatabaseConfig.execute("update person set name = 'Robert' where id = 1");
        response = watcher.getLast();
        assertEquals(LiveResponse.Modify, response.getAction());
        assertEquals("person", response.getTable());
        assertEquals(1, response.getRecords().size());
        results = DatabaseConfig.convertResponse(response);
        assertEquals("Robert", results.get(0).get("name"));
    }
}
