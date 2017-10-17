package com.company;

import org.apache.zookeeper.*;

public class Worker implements Watcher {

    private Long id;
    private ZooKeeper zoo;

    public Worker(ZooKeeper zoo) {
        this.zoo = zoo;
    }

    //Assigns unique ID based on thread ID
    public void assignId() {
        this.id = new Long(Thread.currentThread().getId());
    }

    public void askEnrollment() throws KeeperException, InterruptedException {
        if (null != zoo.exists("/request/enroll/" + id.toString(), null))
            return;
        zoo.create("/request/enroll/" + id.toString(), "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //Setting watcher if state changes
        zoo.exists("/request/enroll/" + id.toString(), this);
    }

    public void confirmEnrollment(String path) {
        try {
            if("1".equals(getData(path)) || "2".equals(getData(path))) {
                System.out.println("Firing Communication (NO DEVELOPED)");
                this.zoo.delete(path,-1);
            }
            else
                //Error in Master. Setting watcher again
                zoo.exists("/request/enroll/" + id.toString(), this);

        } catch (Exception e) { e.printStackTrace(); }
    }

    public void leaveEnrollment() throws KeeperException, InterruptedException {
        if (null != zoo.exists("/request/quit/" + id.toString(), null))
            return;
        zoo.create("/request/quit/" + id.toString(), "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //Setting watcher if state changes
        zoo.getData("/request/quit/" + id.toString(), this, null);
    }

    //Return the value of the node of the specified path, if any error occurrs then returns null
    private String getData(String path) {
        String value = null;
        try {
            value = new String(zoo.getData(path, null, null), "UTF-8");
        } catch (Exception e) {
            value = null;
        }

        return value;
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDeleted)
            System.out.println(watchedEvent.getPath() + " deleted");//Fire data cleaning

        else if (watchedEvent.getType() == Event.EventType.NodeDataChanged)
            if (watchedEvent.getPath().contains("enroll"))
                confirmEnrollment(watchedEvent.getPath());
            else
                System.out.println("ERROR: Event NodeDataChanged detected on" + watchedEvent.getPath());

        else if (watchedEvent.getType() == Event.EventType.NodeCreated)
                id = id; //Fire starting communication

        else
            System.out.println("Error on "+ watchedEvent.getPath() + " with event " + watchedEvent.getType());
    }


    public void run() {
        try {

            assignId();

            askEnrollment();

            Thread.sleep(1000);

            leaveEnrollment();


        } catch (Exception e) { e.printStackTrace(); }
    }
}