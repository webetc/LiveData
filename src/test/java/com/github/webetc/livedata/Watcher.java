package com.github.webetc.livedata;

public class Watcher implements LiveObserver {
    private LiveResponse last = null;

    @Override
    public void send(LiveResponse response) {
        last = response;
    }

    public void reset() {
        last = null;
    }

    public LiveResponse getLast() throws Exception {
        int count = 0;
        while (last == null) {
            Thread.sleep(100);
            count++;
            if (count > 10)
                throw new Exception("Missing expected response");
        }

        LiveResponse response = last;
        last = null;
        return response;
    }
}
