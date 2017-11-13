package com.company;

import org.apache.zookeeper.*;
import java.io.IOException;
import java.util.*;
import org.apache.zookeeper.Watcher.Event.EventType;

public class Worker implements Watcher {

    private static final int SHUT_DOWN = 0;
    //private static final int SIGN_IN = 1;
    private static final int SIGN_UP = 2;

    private static final String NEW_MESSAGE = "N";
    private static final String UNREGISTER = "U";
    private static final String ONLINE_USERS = "O";
    private static final String EXIT = "E";

    private static final String CLOSE = "^C";

    private static Scanner input = new Scanner(System.in);

    private String id;
    private ZooKeeper zoo;

    private Worker(ZooKeeper connection, String id) throws IOException, InterruptedException {
        this.id = id;
        this.zoo = connection;
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        Worker w = null;

        do {

            boolean isLoginOk;
            do {

                System.out.println();
                System.out.print("  [ (0) Shut down | (1) Sign In | (2) Sign Up ] ");
                int inputCommand = input.nextInt();
                input.nextLine();

                if (inputCommand == SHUT_DOWN) {
                    System.out.println(">> Shutting down..");
                    return;
                }

                boolean toEnroll = (inputCommand == SIGN_UP);

                boolean isUsernameOk;
                do {

                    System.out.print("> Username: ");

                    String inputId = input.nextLine().replace(" ", "");

                    if (inputId.equals(CLOSE))
                        break;

                    w = new Worker(ZooHelper.getConnection(), inputId);

                    // if it has to sign up, enroll it first, otherwise assume the username is ok
                    isUsernameOk = !toEnroll || w.enroll();

                } while (!isUsernameOk);

                // sign in (verifying that the user has been initialized and it was indeed registered)
                isLoginOk = ( (w != null) && (w.goOnline()) );

            } while (!isLoginOk);


            do {

                System.out.println();
                System.out.print("  [ (N) New Chat | (O) See Online Users | (D) Deregister | (E) Exit ] ");
                String choice = input.next().toUpperCase();
                input.nextLine();

                if (choice.equals(EXIT)) {
                    System.out.println(">> Exiting...");
                    break;
                }


                if (choice.equals(UNREGISTER) && w.quit()) {
                    System.out.println(">> You have been correctly unregistered.");
                    break;
                }

                if (choice.equals(ONLINE_USERS))
                    Master.getOnlineUsers();

                if (choice.equals(NEW_MESSAGE)) {

                    System.out.println("<< ENTER ^C TO CLOSE THIS CHAT. >>");
                    String idReceiver;
                    boolean close = false;

                    do {
                        System.out.print(">> To: ");
                        idReceiver = input.nextLine().replace(" ", "");

                        if (idReceiver.equals(CLOSE)) {
                            close = true;
                            break;
                        }

                    } while (!w.choosesValidReceiver(idReceiver));

                    if (!close) {
                        do {
                            System.out.print(">>> Text: ");
                            String message = input.nextLine();

                            if (message.equals(CLOSE))
                                break;

                            w.write(idReceiver, message);
                        } while (true);
                    }

                }

            } while (true);

        } while (true);

    }

    /* WORKER'S ACTIONS ***********************************************************************************************/

    /**
     * Creates a node in '/request/enroll/w_id' and set a watcher for async process when it changes
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private boolean enroll() throws KeeperException, InterruptedException {

        String enrollUserPath = "/request/enroll/" + this.id;

        try { zoo.create(enrollUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); }
        catch (KeeperException.NodeExistsException e) {
            ZooHelper.print("<ERROR> Another user is already trying to register with the same username. Please choose another username.");
            return false;
        }

        // set a watcher to be notified when the code changes
        zoo.getData(enrollUserPath, this, null);

        do { Thread.sleep(100); }
        while (ZooHelper.exists(enrollUserPath, zoo) && Arrays.equals(ZooHelper.getCode(enrollUserPath, zoo), ZooHelper.Codes.NEW_CHILD));

        if (!ZooHelper.exists(enrollUserPath, zoo)
                || Arrays.equals(ZooHelper.getCode(enrollUserPath, zoo), ZooHelper.Codes.SUCCESS))
            return true;

        // otherwise there was an error: delete the request anyway
        zoo.delete(enrollUserPath, -1);
        return false;

    }


    /**
     * Creates a node in '/request/quit/w_id' and set a watcher for async process when it changes
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private boolean quit() throws KeeperException, InterruptedException {

        String quitUserPath = "/request/quit/" + this.id;

        zoo.create(quitUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.getData(quitUserPath, this, null); //Setting watcher if code changes

        do { Thread.sleep(100); }
        while (ZooHelper.exists(quitUserPath, zoo) && Arrays.equals(ZooHelper.getCode(quitUserPath, zoo), ZooHelper.Codes.NEW_CHILD));

        if (Arrays.equals(ZooHelper.getCode(quitUserPath, zoo), ZooHelper.Codes.SUCCESS)
                || !ZooHelper.exists(quitUserPath, zoo))
            return true;

        // otherwise there was an error
        return false;
    }

    /**
     * This method takes care of all the procedures for a valid login.
     * @return true for valid login, false for invalid login.
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private boolean goOnline() throws KeeperException, InterruptedException {

        String onlineUserPath = "/online/" + this.id;
        String queueUserPath = "/queue/" + this.id;

        // create a node in "/online" to notify the master
        try { zoo.create(onlineUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL); }
        catch (KeeperException.NodeExistsException e) {
            ZooHelper.print("<ERROR> Either you are trying to login without being registered or you are already online.");
            return false;
        }

        zoo.exists(onlineUserPath, this);

        // wait for the master to take some actions: either delete the node or change its code
        do { Thread.sleep(100); }
        while (ZooHelper.exists(onlineUserPath, zoo) && Arrays.equals(ZooHelper.getCode(onlineUserPath, zoo), ZooHelper.Codes.NEW_CHILD));

        // if the master deleted the online node, then the user wasn't registered
        if (!ZooHelper.exists(onlineUserPath, zoo))
            return false;

        // if there are unread messages for the user, the master moved them to the queue: read them
        List<String> unreadMessages = zoo.getChildren(queueUserPath, false);
        if (unreadMessages.size() > 0) {
            for (String message : unreadMessages) {
                ZooHelper.print("<INFO> New Unread Message: "
                        + message.split(":")[1].replaceAll("[0-9]{10}", ""));
            }
        }

        // once read all the old messages, set watcher for possible new incoming messages
        zoo.getChildren(queueUserPath, this);

        return true;
    }


    private void write(String idReceiver, String message) throws KeeperException, InterruptedException {

        if (!ZooHelper.exists("/online/" + this.id, zoo)) {
            ZooHelper.print("<ERROR> You are not online. Go online first!");
            return;
        }

        zoo.create("/queue/" + idReceiver + "/" + this.id + ":"
                + message, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(ZooHelper.timestamp());
    }

    /*
    private String read() throws KeeperException, InterruptedException {

        if (zoo.exists("/online/" + this.id, null) == null) {
            ZooHelper.print("<ERROR> User " + this.id + " cannot read messages without being online. Go online first!");
            return null;
        }

        if(zoo.exists("/queue/" + this.id, null) == null) {
            ZooHelper.print("<ERROR> User " + this.id + " must have a /queue node where messages can be stored!");
            return null;
        }

        List<String> messages = zoo.getChildren("/queue/"+ this.id, null);

        if (messages.isEmpty()){
            ZooHelper.print("<WARNING> No messages left in " + this.id + "'s inbox.");
            return null;
        }

        //Collections.sort(messages, (left, right) -> Integer.parseInt(left.substring(left.length()-10)) - Integer.parseInt(right.substring(right.length()-10));
        // Good Java :D
        // Comparator used to get the message with lowest ID at a time
        Comparator<String> message_comparator = new Comparator<String>() {
            @Override
            public int compare(String left, String right) {
                return Integer.parseInt(left.substring(left.length()-9)) - Integer.parseInt((right.substring(right.length()-9))); // use your logic
            }
        };

        Collections.sort(messages, message_comparator);
        System.out.println(">>> READ Message for " + this.id + ":" + messages.get(0));
        System.out.println("Number of message remaining: "+ (messages.size()-1));

        String messageID = messages.get(0);
        String messageContent = messageID.split(":")[1].replaceAll("[0-9]{10}", "");
        System.out.println(messageContent + ";");

        // delete the message from the queue as soon as it is read
        zoo.delete("/queue/" + this.id + "/" + messageID, -1);

        return messageContent;
    }


    private String readAll() throws KeeperException, InterruptedException {

        String reply = read();
        String longString = "";

        while(reply != null) {
            reply = read();
            longString = longString + reply;
        }

        return longString;

    }
    */

    /* WATCHER'S METHODS **********************************************************************************************/

    /**
     * This process is inherited from Watcher interface. It is fired each time a watcher (that was set in a node)
     * changed or some children were created in the path (depending on how the watcher was set).
     * Ref: https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
     *
     * It contains the asynchronous logic of the worker. The general case in which this watcher is fired is after the
     * master has evaluated the initial worker's request and it sends back the result of the request by changing the code
     * associated with the node just created. The watcher here must then interpret the new codes and act accordingly.
     *
     * @param watchedEvent Event triggered containing path type and state
     */
    @Override
    public void process(WatchedEvent watchedEvent) {

        EventType triggerEvent = watchedEvent.getType();
        String triggerPath = watchedEvent.getPath();
        //byte[] triggerCode = ZooHelper.getCode(triggerPath, zoo);

        // NEW ENROLLMENT/QUIT REQUEST RESULT
        if (triggerPath.contains("/request") && triggerEvent == EventType.NodeDataChanged) {
            byte[] requestCode = ZooHelper.getCode(triggerPath, zoo);
            String requestType = triggerPath.split("/")[2];

            if (Arrays.equals(requestCode, ZooHelper.Codes.SUCCESS)) {

                ZooHelper.print("<INFO> You managed  to " + requestType + " successfully!");
                try {
                    zoo.delete(triggerPath, -1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }

            } else if (Arrays.equals(requestCode, ZooHelper.Codes.NODE_EXISTS)) {

                if (ZooHelper.exists("/online/" + this.id, zoo))
                    ZooHelper.print("<WARNING> You are already online.");
                else
                    ZooHelper.print("<WARNING> This username has already been taken.");


            } else if (Arrays.equals(requestCode, ZooHelper.Codes.EXCEPTION)) {

                ZooHelper.print("<ERROR> It was impossible to " + requestType + " due to unknown/unexpected reasons.");

            }

            return;
        }

        // NEW ONLINE REQUEST RESULT
        if (triggerPath.contains("/online") && triggerEvent == EventType.NodeDeleted) {
            ZooHelper.print("<WARNING> You cannot go online without being registered. Try to register first.");
            return;
        }

        // NEW MESSAGE RECEIVED
        if (triggerPath.contains("/queue/" + this.id) && triggerEvent == EventType.NodeChildrenChanged) {
            try {
                String nodeId = zoo.getChildren(triggerPath, false).get(0);
                ZooHelper.print(ZooHelper.getSender(nodeId) + ": " + ZooHelper.getMessage(nodeId));

                // after having read the message, set the watcher for the next one
                zoo.getChildren("/queue/" + this.id, this);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //return;
        }

    }

    private boolean choosesValidReceiver(String idReceiver){

        if (!ZooHelper.exists("/online/" + idReceiver, this.zoo)) {
            ZooHelper.print("<ERROR> The receiver is not online. You cannot write to offline people!");
            return false;
        }

        if (idReceiver.equals(this.id)) {
            ZooHelper.print("<ERROR> You cannot write to yourself!");
            return false;
        }

        return true;
    }


}