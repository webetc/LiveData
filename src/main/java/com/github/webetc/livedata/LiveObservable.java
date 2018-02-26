package com.github.webetc.livedata;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class LiveObservable {
    private List<LiveObserver> watchers = new CopyOnWriteArrayList<>();

    public void addWatcher(LiveObserver o) {
        watchers.add(o);
    }


    public void removeWatcher(LiveObserver o) {
        watchers.remove(o);
    }


    public void notifyWatchers(LiveResponse response) {
        for (LiveObserver o : watchers) {
            o.send(response);
        }
    }
}
