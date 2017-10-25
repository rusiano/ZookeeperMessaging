package com.company;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Arrays;

public class Worker implements Watcher, Runnable {

    private String id;
    private ZooKeeper zoo;

    Worker(String HOST, String id) throws IOException, InterruptedException {
        this.zoo = ZooMsg.setupConnection(HOST);
        this.id = (id == null || id.equals("")) ? String.valueOf(Thread.currentThread().getId()) : id;
    }

    /**
     * Creates a node in '/request/enroll/w_id' and set a watcher for async process when it changes
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private void askEnrollment() throws KeeperException, InterruptedException {

        //If node exists, it returns without creating another one
        if (null != zoo.exists("/request/enroll/" + id, null))
            return;

        zoo.create("/request/enroll/" + id, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //Setting watcher if state changes
        zoo.exists("/request/enroll/" + id, this);

    }

    /**
     * Method that manages the data after the watcher of '/request/enroll/w_id' is fired
     */
    private void confirmEnrollment(String path) {

        try {
            if(Arrays.equals(ZooMsg.Codes.SUCCESS, ZooMsg.getNodeCode(zoo, path))
                    || Arrays.equals(ZooMsg.Codes.NODE_EXISTS, ZooMsg.getNodeCode(zoo, path))) {
                System.out.println("-Firing Communication (NOT DEVELOPED YET)-");
                this.zoo.delete(path,-1);
            }
            else
                //Error in Master. Setting watcher again
                zoo.exists("/request/enroll/" + id, this);

        } catch (Exception e) { e.printStackTrace(); }
        System.out.println("Worker "+id + " confirm its enrollment");

    }


    /**
     * Creates a node in '/request/quit/w_id' and set a watcher for async process when it changes
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private void leaveEnrollment() throws KeeperException, InterruptedException {
        //If node exists, it returns without creating another one
        if (null != zoo.exists("/request/quit/" + id, null))
            return;
        zoo.create("/request/quit/" + id, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //Setting watcher if state changes
        zoo.getData("/request/quit/" + id, this, null);
    }

    /**
     * Method that manages the data after the watcher of '/request/quit/w_id' is fired
     */
    private void confirmRemoval(String path){
        try {
            if(Arrays.equals(ZooMsg.Codes.SUCCESS, ZooMsg.getNodeCode(zoo, path))
                    || Arrays.equals(ZooMsg.Codes.NODE_EXISTS, ZooMsg.getNodeCode(zoo, path))) {
                System.out.println("-Firing Cleaning (NOT DEVELOPED YET)-");
                this.zoo.delete(path,-1);
            }
            else {
                System.out.println("Error in Master. Setting watcher again");
                zoo.exists("/request/quit/" + id, this);
            }
        } catch (Exception e) { e.printStackTrace(); }
        System.out.println("Worker "+ id + " confirm its removal");

    }

    /**
     * This process is inherited from Watcher interface. It is fired each time a watcher (that was set in a node)
     * changed or some children were created in the path (depending on how the watcher was set). Ref: https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
     *
     * It contains the asynchronous logic of the worker i.e. the request of enrollment has been processed
     * @param watchedEvent Event triggered containing path type and state
     */
    @Override
    public void process(WatchedEvent watchedEvent) {

        if (watchedEvent.getType() == Event.EventType.NodeDataChanged)
            if (watchedEvent.getPath().contains("enroll"))    // Node Change in enrollment (path)
                confirmEnrollment(watchedEvent.getPath());
            else if (watchedEvent.getPath().contains("quit")) // Node Change in quit (path)
                confirmRemoval(watchedEvent.getPath());
            else
                System.out.println("ERROR: Event NodeDataChanged detected on" + watchedEvent.getPath());
        else
            System.out.println("Error on "+ watchedEvent.getPath() + " with event " + watchedEvent.getType());
    }


    /**
     * Main method from runnable (independent process)
     */
    @Override
    public void run() {
        try {

            this.askEnrollment();

            System.out.println("Worker "+id + "waiting for getting accepted");

            //After requiring enrolment, the process sleeps a little
            for(int i = 0; i < 10; i++)
                Thread.sleep(100);

            leaveEnrollment();

        } catch (Exception e) { e.printStackTrace(); }
    }
}