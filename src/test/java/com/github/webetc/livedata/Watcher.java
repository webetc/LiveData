package com.github.webetc.livedata;

import java.util.ArrayList;
import java.util.List;

public class Watcher implements LiveObserver {
    private LiveResponse last = null;
    private List<LiveResponse> responseList = new ArrayList<>();

    @Override
    public void send(LiveResponse response) {
        synchronized (responseList) {
            last = response;
            responseList.add(response);
        }
    }

    public void reset() {
        synchronized (responseList) {
            last = null;
            responseList.clear();
        }
    }

    public LiveResponse getLast() throws Exception {
        List<LiveResponse> responses = get(1);
        return responses.get(responses.size() - 1);
    }

    public List<LiveResponse> get(int num) throws Exception {
        int count = 0;
        int lim = 5 + (5*num);
        while (responseList.size() < num) {
            Thread.sleep(100);
            count++;
            if (count > lim)
                throw new Exception("Missing expected responses");
        }

        List<LiveResponse> responses = new ArrayList<>();
        synchronized (responseList) {
            responses.addAll(responseList);
        }
        reset();
        return responses;
    }
}
