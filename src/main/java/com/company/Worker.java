package com.company;

import com.google.common.collect.Lists;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.Watcher.Event.EventType;

public class Worker implements Watcher {

    private String id;
    private ZooKeeper zoo;

    Worker(String HOST, String id) throws IOException, InterruptedException {
        this.zoo = ZooMsg.setupConnection(HOST);
        this.id = id;
    }

    String getId() {
        return id;
    }

    /* WORKER'S ACTIONS ***********************************************************************************************/

    /**
     * Creates a node in '/request/enroll/w_id' and set a watcher for async process when it changes
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    void enroll() throws KeeperException, InterruptedException {

        zoo.create("/request/enroll/" + this.id, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //Setting watcher if state changes
        zoo.exists("/request/enroll/" + this.id, this);

        for(int i = 0; i < 2; i++)
            Thread.sleep(1000);
    }

    /**
     * Creates a node in '/request/quit/w_id' and set a watcher for async process when it changes
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    void quit() throws KeeperException, InterruptedException {

        zoo.create("/request/quit/" + id, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //Setting watcher if state changes
        zoo.getData("/request/quit/" + id, this, null);

        for(int i = 0; i < 2; i++)
            Thread.sleep(1000);
    }


    void goOnline() throws KeeperException, InterruptedException {
        System.out.println(">>> " + this.id + ": ONLINE.");
        zoo.create("/online/" + this.id, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    void goOffline() throws KeeperException, InterruptedException {
        System.out.println(">>> " + this.id + ": OFFLINE.");
        zoo.delete("/online/" + this.id, -1);

        for(int i = 0; i < 2; i++)
            Thread.sleep(1000);
    }


    void write(String idReceiver, String message) throws KeeperException, InterruptedException {

        if (zoo.exists("/online/" + this.id, null) == null) {
            System.out.println("<ERROR> User " + this.id + " cannot send messages without being online. Go online first!");
            return;
        }

        System.out.println(">>> " + this.id + " -> " + idReceiver + ": NEW MESSAGE.}");
        zoo.create("/queue/" + idReceiver + "/msg:" + message, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        for(int i = 0; i < 2; i++)
            Thread.sleep(1000);
    }

    void read() throws KeeperException, InterruptedException {

        if (zoo.exists("/online/" + this.id, null) == null) {
            System.out.println("<ERROR> User " + this.id + " cannot read messages without being online. Go online first!");
            return;
        }

        System.out.println(">>> READ First Message for " + this.id + ":");
        List<String> messages = zoo.getChildren("/queue/"+ this.id, null);
        Collections.sort(messages);
        String messageID = messages.get(messages.size()-1);
        String messageContent = messageID.split(":")[1].replaceAll("[0-9]{10}", "");
        System.out.println(messageContent + ";");

        // delete the message from the queue as soon as it is read
        zoo.delete("/queue/" + this.id + "/" + messageID, -1);

        for(int i = 0; i < 2; i++)
            Thread.sleep(1000);
    }

    void readAll() throws KeeperException, InterruptedException {

        if (zoo.exists("/online/" + this.id, null) == null) {
            System.out.println("<ERROR> User " + this.id + " cannot read messages without being online. Go online first!");
            return;
        }

        System.out.println(">>> READ Messages for " + this.id + ":");

        List<String> messages = zoo.getChildren("/queue/"+ this.id, null);
        for (String messageID : Lists.reverse(messages)) {
            System.out.print(messageID.split(":")[1].replaceAll("[0-9]{10}", "") + "; ");

            // delete each message from the queue as soon as it is read
            zoo.delete("/queue/" + this.id + "/" + messageID, -1);
        }

        System.out.println();

        for(int i = 0; i < 2; i++)
            Thread.sleep(1000);
    }


    /* WATCHERS' METHODS **********************************************************************************************/

    /**
     * This process is inherited from Watcher interface. It is fired each time a watcher (that was set in a node)
     * changed or some children were created in the path (depending on how the watcher was set).
     * Ref: https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
     *
     * It contains the asynchronous logic of the worker i.e. the request of enrollment has been processed
     * @param watchedEvent Event triggered containing path type and state
     */
    @Override
    public void process(WatchedEvent watchedEvent) {

        EventType triggerEvent = watchedEvent.getType();
        String triggerPath = watchedEvent.getPath();
        byte[] triggerCode = ZooMsg.getNodeCode(zoo, triggerPath);

        // If the watcher was not triggered for NodeDataChanged warn the user and QUIT.
        if (triggerEvent != EventType.NodeDataChanged) {
            System.out.println("<WARNING> Unhandled event " + triggerEvent + " detected at " + triggerPath + ".");
            return;
        }

        try {

            // If the enrollment/quit was not successful warn the user, set again the watcher and QUIT.
            if (!Arrays.equals(ZooMsg.Codes.SUCCESS, triggerCode)
                    && !Arrays.equals(ZooMsg.Codes.NODE_EXISTS, triggerCode)) {

                System.out.println("<WARNING> Unsuccessful request at " + triggerPath + " with code " + new String(triggerCode) + "."
                        + " Setting new watcher at " + triggerPath + "/" + this.id + ".");
                zoo.exists(triggerPath + "/" + this.id, this);
                return;

            }

            // Otherwise the (de)registration was successful: remove the old node in enroll(quit)
            zoo.delete(triggerPath, -1);


        } catch (Exception e) { e.printStackTrace(); }

    }


    /* Main method from runnable (independent process)

    @Override
    public void run() {
        try {

            this.enroll();

            System.out.println("Worker "+id + "waiting for getting accepted");

            //After requiring enrolment, the process sleeps a little
            for(int i = 0; i < 10; i++)
                Thread.sleep(100);

            this.goOnline();
            //quit();

        } catch (Exception e) { e.printStackTrace(); }
    }*/
}