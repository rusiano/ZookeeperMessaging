package com.company;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public class BasicWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {

        if (event.getType() == EventType.NodeCreated) {
            ZooMsg.out.println(event.getPath() + " created");
        } else if (event.getType() == EventType.NodeDeleted) {
            ZooMsg.out.println(event.getPath() + " deleted");
        } else if (event.getType() == EventType.NodeDataChanged) {
            ZooMsg.out.println(event.getPath() + " changed");
        } else if (event.getType() == EventType.NodeChildrenChanged) {
            ZooMsg.out.println(event.getPath() + " children created");
        } else {
            ZooMsg.out.println(event.getPath() + " what is this??");
        }

    }

}
