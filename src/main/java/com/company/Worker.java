package com.company;

import com.google.common.collect.Lists;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

        //Blocking execution until enrollment finish (deleting node => node == null)
        while(zoo.exists("/request/enroll/" + this.id, null) != null)
            Thread.sleep(100);
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

        //Blocking execution until enrollment finish (deleting node => node == null)
        while(zoo.exists("/request/quit/" + this.id, null) != null)
            Thread.sleep(100);
    }


    void goOnline() throws KeeperException, InterruptedException {
        zoo.create("/online/" + this.id, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        //Blocking execution until online operation finish (master changes node => node value == successful)
        while(!Arrays.equals(ZooMsg.Codes.SUCCESS, zoo.getData("/online/" + this.id, null, null))) {
            Thread.sleep(100);
        }
        System.out.println(">>> " + this.id + ": ONLINE.");
    }

    void goOffline() throws KeeperException, InterruptedException {
        zoo.delete("/online/" + this.id, -1);

        for(int i = 0; i < 2; i++)
            Thread.sleep(1000);

        System.out.println(">>> " + this.id + ": OFFLINE.");
    }


    void write(String idReceiver, String message) throws KeeperException, InterruptedException {

        if (zoo.exists("/online/" + this.id, null) == null) {
            System.out.println("<ERROR> User " + this.id + " cannot send messages without being online. Go online first!");
            return;
        }

        System.out.println(">>> " + this.id + " -> " + idReceiver + ": NEW MESSAGE.}");
        zoo.create("/queue/" + idReceiver + "/msg:" + message, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        for(int i = 0; i < 2; i++)
            Thread.sleep(100);
    }

    /**
     * Read the first message according the sequential code of the znode stored in /queue/ID
     * @return String if there is a message or null if there is no message available/error
     *
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    String read() throws KeeperException, InterruptedException {

        if (zoo.exists("/online/" + this.id, null) == null) {
            System.out.println("<ERROR> User " + this.id + " cannot read messages without being online. Go online first!");
            return null;
        }

        if(zoo.exists("/queue/" + this.id, null) == null) {
            System.out.println("<ERROR> User " + this.id + " must have a /queue node where messages can be stored!");
            return null;
        }

        List<String> messages = zoo.getChildren("/queue/"+ this.id, null);

        if (messages.isEmpty()){
            System.out.println("<WARNING> User " + this.id + " tried to read without having messages in queue.");
            return null;
        }

        //Collections.sort(messages, (left, right) -> Integer.parseInt(left.substring(left.length()-10)) - Integer.parseInt(right.substring(right.length()-10)); //Good Java :D
        // Comparator used to get the message with lowest ID at a time
        Comparator<String> message_comparator = new Comparator<String>() {
            @Override
            public int compare(String left, String right) {
                return Integer.parseInt(left.substring(left.length()-9)) - Integer.parseInt((right.substring(right.length()-9))); // use your logic
            }
        };

        Collections.sort(messages, message_comparator);
        System.out.println(">>> READ First Message for " + this.id + ":" + messages.get(0));
        System.out.println("Number of message remaining: "+ (messages.size()-1));

        String messageID = messages.get(0);
        String messageContent = messageID.split(":")[1].replaceAll("[0-9]{10}", "");
        System.out.println(messageContent + ";");

        // delete the message from the queue as soon as it is read
        zoo.delete("/queue/" + this.id + "/" + messageID, -1);

        return messageContent;
    }


    String readAll() throws KeeperException, InterruptedException {

        String reply = read();
        String longString = "";

        while(reply != null) {
            reply = read();
            longString = longString + reply;
        }

        return longString;

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