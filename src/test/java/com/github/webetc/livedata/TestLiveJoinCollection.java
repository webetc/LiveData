package com.github.webetc.livedata;

import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestLiveJoinCollection {

    private Watcher watcher = new Watcher();
    private LiveJoinCollection collection;
    private List<LiveResponse> responses;


    @Before
    public void before() throws Exception {
        DatabaseConfig.reloadSchema();
        watcher.reset();
        collection = new LiveJoinCollection(
                DatabaseConfig.getSchemaName(),
                "person",
                "name",
                DatabaseConfig.getDatabase(),
                watcher
        );
        collection.join("phone", "userId");
    }


    @After
    public void after() throws Exception {
        if (collection != null)
            collection.close();
    }


    @org.junit.Test
    public void test_add_constraint() throws Exception {
        collection.addConstraint("Bob");
        responses = watcher.get(2);
        boolean havePerson = false;
        boolean havePhone = false;
        for (LiveResponse response : responses) {
            assertEquals(LiveResponse.Modify, response.getAction());
            if (response.getTable().equals("person"))
                havePerson = true;
            if (response.getTable().equals("phone"))
                havePhone = true;
            assertEquals(1, response.getRecords().size());
        }
        assertTrue("person", havePerson);
        assertTrue("phone", havePhone);
    }


    @org.junit.Test
    public void test_add_remove_constraints() throws Exception {
        // Add
        String[] constraints = {"Bob", "Chris"};
        collection.addConstraints(Arrays.asList(constraints));
        responses = watcher.get(2);
        int havePerson = 0;
        int havePhone = 0;
        for (LiveResponse response : responses) {
            assertEquals(LiveResponse.Modify, response.getAction());
            if (response.getTable().equals("person"))
                havePerson += response.getRecords().size();
            if (response.getTable().equals("phone"))
                havePhone += response.getRecords().size();
            assertEquals(2, response.getRecords().size());
        }
        assertEquals("person", 2, havePerson);
        assertEquals("phone", 2, havePhone);

        // Remove
        collection.removeConstraints(Arrays.asList(constraints));
        responses = watcher.get(2);
        havePerson = 0;
        havePhone = 0;
        for (LiveResponse response : responses) {
            assertEquals(LiveResponse.Delete, response.getAction());
            if (response.getTable().equals("person"))
                havePerson += response.getRecords().size();
            if (response.getTable().equals("phone"))
                havePhone += response.getRecords().size();
            assertEquals(2, response.getRecords().size());
        }
        assertEquals("person", 2, havePerson);
        assertEquals("phone", 2, havePhone);
    }

}
