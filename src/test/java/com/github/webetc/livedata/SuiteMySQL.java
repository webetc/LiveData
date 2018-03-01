package com.github.webetc.livedata;

import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestMySQL.class,
        TestLiveTable.class,
        TestLiveKeyCollection.class,
        TestLiveJoinCollection.class
})
public class SuiteMySQL {
    @ClassRule
    public static ExternalResource getResource() {
        return new ExternalResource() {
            @Override
            protected void before() throws Throwable {
                DatabaseConfig.setup();
            }

            @Override
            protected void after() {
                try {
                    DatabaseConfig.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }
}
