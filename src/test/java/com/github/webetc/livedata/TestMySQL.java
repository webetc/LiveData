package com.github.webetc.livedata;

import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestMySQL {

    private List<Map<String, String>> results;


    @Before
    public void before() throws Exception {
        DatabaseConfig.reloadSchema();
    }


    @org.junit.Test
    public void test_db_setup() throws Exception {
        results = DatabaseConfig.query("select * from person where name = 'Bob'");
        assertEquals("Results size", 1, results.size());
        assertEquals("1", results.get(0).get("id"));
    }


    @org.junit.Test
    public void test_bin_log() throws Exception {
        results = DatabaseConfig.query("SHOW BINARY LOGS");
        assertTrue("Results size", results.size() > 0);
    }
}
