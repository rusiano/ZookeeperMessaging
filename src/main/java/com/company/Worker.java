package com.company;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Arrays;

public class Worker implements Watcher, Runnable {

    private String id;
    private ZooKeeper zoo;

    Worker() throws IOException, InterruptedException {
        this.zoo = ZooMsg.setupConnection();
        this.id = String.valueOf(Thread.currentThread().getId());
    }

    private void askEnrollment() throws KeeperException, InterruptedException {

        if (null != zoo.exists("/request/enroll/" + id, null))
            return;

        zoo.create("/request/enroll/" + id, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //Setting watcher if state changes
        zoo.exists("/request/enroll/" + id, this);

    }

    private void confirmEnrollment(String path) {

        try {
            if(Arrays.equals(ZooMsg.Codes.SUCCESS, ZooMsg.getNodeCode(zoo, path))
                    || Arrays.equals(ZooMsg.Codes.NODE_EXISTS, ZooMsg.getNodeCode(zoo, path))) {
                System.out.println("-Firing Communication (NO DEVELOPED)-");
                this.zoo.delete(path,-1);
            }
            else
                //Error in Master. Setting watcher again
                zoo.exists("/request/enroll/" + id, this);

        } catch (Exception e) { e.printStackTrace(); }
        System.out.println("Worker "+id + "confirm its enrollment");

    }

    private void confirmRemoval(String path) {

        try {

            if(Arrays.equals(ZooMsg.Codes.SUCCESS, ZooMsg.getNodeCode(zoo, path))
                    || Arrays.equals(ZooMsg.Codes.NODE_EXISTS, ZooMsg.getNodeCode(zoo, path))) {
                System.out.println("-Firing Cleaning (NO DEVELOPED)-");
                this.zoo.delete(path,-1);
            }
            else
                //Error in Master. Setting watcher again
                zoo.exists("/request/quit/" + id, this);

        } catch (Exception e) { e.printStackTrace(); }

        System.out.println("Worker "+ id + "confirm its removal");

    }

    private void leaveEnrollment() throws KeeperException, InterruptedException {
        if (null != zoo.exists("/request/quit/" + id, null))
            return;
        zoo.create("/request/quit/" + id, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //Setting watcher if state changes
        zoo.getData("/request/quit/" + id, this, null);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        //System.out.println("Evento"+watchedEvent.getPath()+watchedEvent.getType());

        if (watchedEvent.getType() == Event.EventType.NodeDeleted)
            System.out.println(watchedEvent.getPath() + " deleted");//Fire data cleaning

        else if (watchedEvent.getType() == Event.EventType.NodeDataChanged)
            if (watchedEvent.getPath().contains("enroll"))
                confirmEnrollment(watchedEvent.getPath());
            else if (watchedEvent.getPath().contains("quit"))
                confirmRemoval(watchedEvent.getPath());
            else
                System.out.println("ERROR: Event NodeDataChanged detected on" + watchedEvent.getPath());

        else if (watchedEvent.getType() == Event.EventType.NodeCreated)
            id = id; //Fire starting communication

        else
            System.out.println("Error on "+ watchedEvent.getPath() + " with event " + watchedEvent.getType());
    }

    @Override
    public void run() {
        try {

            this.askEnrollment();
            System.out.println("Worker "+id + "waiting for getting accepted");
            for(int i = 0; i < 1; i++)
                Thread.sleep(1000);

            leaveEnrollment();


        } catch (Exception e) { e.printStackTrace(); }
    }
}