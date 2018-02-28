package com.github.webetc.livedata;

import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestLiveKeyCollection {

    private Watcher watcher = new Watcher();
    private LiveKeyCollection collection;
    private LiveResponse response;
    private List<Map<String, String>> results;


    @Before
    public void before() throws Exception {
        DatabaseConfig.reloadSchema();
        watcher.reset();
        collection = DatabaseConfig.createCollection("phone", "userId", "1", watcher);
        response = watcher.getLast();
        assertEquals(LiveResponse.Load, response.getAction());
        assertEquals("phone", response.getTable());
        assertEquals(1, response.getRecords().size());
    }


    @After
    public void after() throws Exception {
        if (collection != null)
            collection.close();
    }


    @org.junit.Test
    public void test_insert() throws Exception {
        DatabaseConfig.execute("insert into phone(userId, phoneNumber) values(1, '555-555-2222')");
        response = watcher.getLast();
        assertEquals(LiveResponse.Modify, response.getAction());
        assertEquals("phone", response.getTable());
        assertEquals(1, response.getRecords().size());
        results = DatabaseConfig.convertResponse(response);
        assertEquals("555-555-2222", results.get(0).get("phoneNumber"));
    }


    @org.junit.Test
    public void test_update() throws Exception {
        results = DatabaseConfig.query("select * from phone where userId = 1");
        assertEquals(1, results.size());
        String phoneId = results.get(0).get("id");
        DatabaseConfig.execute("update phone set phoneNumber = '555-555-9999' where id = " + phoneId);
        response = watcher.getLast();
        assertEquals(LiveResponse.Modify, response.getAction());
        assertEquals("phone", response.getTable());
        assertEquals(1, response.getRecords().size());
        results = DatabaseConfig.convertResponse(response);
        assertEquals("555-555-9999", results.get(0).get("phoneNumber"));
        assertEquals(phoneId, results.get(0).get("id"));
    }


    @org.junit.Test
    public void test_delete() throws Exception {
        results = DatabaseConfig.query("select * from phone where userId = 1");
        assertEquals(1, results.size());
        String phoneId = results.get(0).get("id");
        DatabaseConfig.execute("delete from phone where id = " + phoneId);
        response = watcher.getLast();
        assertEquals(LiveResponse.Delete, response.getAction());
        assertEquals("phone", response.getTable());
        assertEquals(1, response.getRecords().size());
        results = DatabaseConfig.convertResponse(response);
        assertEquals(phoneId, results.get(0).get("id"));
    }


    @org.junit.Test
    public void test_add_remove_constraint() throws Exception {
        collection.addConstraint("2");
        response = watcher.getLast();
        assertEquals(LiveResponse.Modify, response.getAction());
        assertEquals("phone", response.getTable());
        assertEquals(1, response.getRecords().size());
        results = DatabaseConfig.convertResponse(response);
        assertEquals("2", results.get(0).get("userId"));

        // remove
        collection.removeConstraint("1");
        results = DatabaseConfig.query("select * from phone where userId = 1");
        assertEquals(1, results.size());
        String phoneId = results.get(0).get("id");
        DatabaseConfig.execute("update phone set phoneNumber = '555-555-9999' where id = " + phoneId);
        try {
            response = null;
            response = watcher.getLast();
        } catch (Exception e) {
            // expected from watcher
        }
        // not watching user id 1 now so shouldn't get anything in collection
        assertNull(response);
    }
}
