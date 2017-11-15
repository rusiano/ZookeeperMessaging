package com.company;

import org.apache.zookeeper.*;
import java.io.IOException;
import java.util.*;
import org.apache.zookeeper.Watcher.Event.EventType;

public class Worker implements Watcher {

    private static final int SHUT_DOWN = 0;
    private static final int SIGN_UP = 1;

    private static final String NEW_MESSAGE = "N";
    private static final String UNREGISTER = "U";
    private static final String ONLINE_USERS = "O";
    private static final String EXIT = "E";

    private static final String CLOSE = "^C";

    private static Scanner input = new Scanner(System.in);

    private String id;
    private ZooKeeper zoo;
    private boolean isLoginOk, isUsernameOk;

    public Worker(ZooKeeper connection, String id) throws IOException, InterruptedException {
        this.id = id;
        this.zoo = connection;
        this.isLoginOk = false;
        this.isUsernameOk = false;
    }

    private boolean isLoginOk() {
        return this.isLoginOk;
    }

    private boolean isUsernameOk() {
        return this.isUsernameOk;
    }

    private void setLoginOk(boolean loginOk) {
        this.isLoginOk = loginOk;
    }

    private void setUsernameOk(boolean usernameOk) {
        this.isUsernameOk = usernameOk;
    }

    private String getId() {
        return this.id;
    }

    private ZooKeeper getZoo() {
        return this.zoo;
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        Worker user;

        do {

            do {

                System.out.println();
                System.out.print("  [ (0) Shut down | (1) Sign Up | (2) Sign In ] ");
                int inputCommand = input.nextInt();
                input.nextLine();

                if (inputCommand == SHUT_DOWN) {
                    System.out.println(">> Shutting down..");
                    return;
                }

                boolean toEnroll = (inputCommand == SIGN_UP);

                do {

                    System.out.println("<< ENTER ^C TO CLOSE THIS PROMPT. >>");
                    System.out.print("> Username: ");

                    String inputId = input.nextLine().replace(" ", "");

                    if (inputId.equals(CLOSE)) {
                        user = null;
                        break;
                    }

                    user = new Worker(ZooHelper.getConnection(), inputId);

                    // if it has to sign up, enroll it first, otherwise assume the username is ok
                    user.setUsernameOk(!toEnroll || user.enroll());

                } while (!user.isUsernameOk());

                // sign in (verifying that the user has been initialized and it was indeed registered)
                if (user != null) {
                    if (toEnroll) {
                        System.out.println();
                        System.out.println("=== REGISTRATION SUCCESSFUL ===");
                    }

                    user.setLoginOk( user.goOnline() );
                }


            } while (user == null || !user.isLoginOk());

            System.out.println("=== LOGIN SUCCESSFUL ===");
            do {

                System.out.println();
                System.out.print("  [ (N) New Chat | (O) See Online Users | (U) Unregister | (E) Exit ] ");
                String choice = input.next().toUpperCase();
                input.nextLine();

                if (choice.equals(EXIT)) {
                    user.getZoo().delete("/online/" + user.getId(), -1);
                    System.out.println(">> Exiting (going offline)...");
                    break;
                }


                if (choice.equals(UNREGISTER) && user.quit()) {
                    System.out.println(">> You have been correctly unregistered.");
                    break;
                }

                if (choice.equals(ONLINE_USERS))
                    Master.getOnlineUsers();

                if (choice.equals(NEW_MESSAGE)) {

                    System.out.println("<< ENTER ^C TO CLOSE THIS PROMPT. >>");
                    String idReceiver;
                    boolean close = false;

                    do {
                        System.out.print(">> To: ");
                        idReceiver = input.nextLine().replace(" ", "");

                        if (idReceiver.equals(CLOSE)) {
                            close = true;
                            break;
                        }

                    } while (!user.choosesValidReceiver(idReceiver));

                    if (!close) {
                        do {
                            System.out.print(">>> Text: ");
                            String message = input.nextLine();

                            if (message.equals(CLOSE))
                                break;

                            user.write(idReceiver, message);
                        } while (true);
                    }

                }

            } while (true);

        } while (true);

    }

    /**
     * The method checks if the receiver specified is valid for a given user. More specifically, the receiver must be online
     * and cannot be the sender itself.
     * @param idReceiver The id of the receiver to be checked.
     * @return true or false depending if the receiver in input is valid or not.
     */
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

    /* WORKER'S ACTIONS ***********************************************************************************************/

    /**
     * The method manages all the enrollment procedure for the client.
     * The client must create a node in "/request/enroll" and wait for the master to set the appropriate value.
     * In any case the request node is deleted at the end of the process.
     * @return true or false depending if the enrollment was successful or not.
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    public boolean enroll() throws KeeperException, InterruptedException {

        boolean validEnroll;
        String enrollUserPath = "/request/enroll/" + this.id;

        zoo.create(enrollUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.getData(enrollUserPath, this, null);

        do { Thread.sleep(50); }
        while (Arrays.equals(ZooHelper.getCode(enrollUserPath, zoo), ZooHelper.Codes.NEW_CHILD));

        validEnroll = Arrays.equals(ZooHelper.getCode(enrollUserPath, zoo), ZooHelper.Codes.SUCCESS);

        zoo.delete(enrollUserPath, -1);
        return validEnroll;

    }


    /**
     * The method manages all the deregistration procedure for the client.
     * Specifically, the client creates a node in "/request/quit" and waits for the master to set the appropriate value.
     * If the master can successfully delete the user from the registry, the method also checks if the user was online
     * and removes the node. In any case the request node is deleted at the end of the process.
     * Please notice that this method does not deleted the queue to let the master move it to backup. The master will
     * take care of deleting the queue at the appropriate time.
     * @return true or false depending if the deregistration was successful or not.
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    public boolean quit() throws KeeperException, InterruptedException {

        boolean validQuit;
        String quitUserPath = "/request/quit/" + this.id;
        String onlineUserPath = "/online/" + this.id;

        zoo.create(quitUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.getData(quitUserPath, this, null); //Setting watcher if code changes

        do { Thread.sleep(50); }
        while (Arrays.equals(ZooHelper.getCode(quitUserPath, zoo), ZooHelper.Codes.NEW_CHILD));

        if (Arrays.equals(ZooHelper.getCode(quitUserPath, zoo), ZooHelper.Codes.SUCCESS)) {

            // delete the node in online, but leave the queue: the master will delete if after having moved the messages
            if (ZooHelper.exists(onlineUserPath, zoo)) zoo.delete(onlineUserPath, -1);
            validQuit = true;

        } else {
            validQuit = false;
        }

        zoo.delete(quitUserPath, -1);
        return validQuit;
    }

    /**
     * This method tries to make the user go online.
     * Specifically, the method tries to create a node in "/online" and waits for the master to either delete the node
     * or set the appropriate success code. In case the login is successful, the method retrieves and shows all unread
     * messages (if any) and finally sets the initial watcher for incoming messages.
     * @return true or false depending if the login was successful or not.
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    public boolean goOnline() throws KeeperException, InterruptedException {

        String onlineUserPath = "/online/" + this.id;
        String queueUserPath = "/queue/" + this.id;

        // create a node in "/online" to notify the master
        try {
            zoo.create(onlineUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            ZooHelper.print("<ERROR> Either you are trying to login without being registered or you are already online.");
            return false;
        }

        zoo.exists(onlineUserPath, this);

        // wait for the master to take some actions: either delete the node or change its code
        do { Thread.sleep(50); }
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


    /**
     * The method sends a message to the specified user.
     * Specifically, creates an ephemeral sequential znode in the queue of the receiver with an id of the form:
     * 'idSender:message'.
     * @param idReceiver The id of the receiver. Please notice that the validity of this parameter must be checked prior
     *                   to the invocation of this method.
     * @param message The content of the message.
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    public void write(String idReceiver, String message) throws KeeperException, InterruptedException {

        if (!ZooHelper.exists("/online/" + this.id, zoo)) {
            ZooHelper.print("<ERROR> You are not online. Go online first!");
            return;
        }

        zoo.create("/queue/" + idReceiver + "/" + this.id + ":"
                + message, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(ZooHelper.timestamp());
    }

    /**
     * The method process a message sent to the process and detected by the watcher.
     */
    public void read(String sender, String message){
        ZooHelper.print(sender  + ": " +  message);
    }

    /* WATCHER'S METHODS **********************************************************************************************/

    /**
     * This process is inherited from Watcher interface. It is fired each time a watcher (that was set in a node)
     * changed or some children were created in the path (depending on how the watcher was set).
     * Ref: https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
     * @param watchedEvent Event triggered containing path type and state
     */
    @Override
    public void process(WatchedEvent watchedEvent) {

        EventType triggerEvent = watchedEvent.getType();
        String triggerPath = watchedEvent.getPath();
        byte[] triggerCode = ZooHelper.getCode(triggerPath, zoo);

        // NEW ENROLLMENT/QUIT REQUEST RESULT
        if (triggerPath.contains("/request") && triggerEvent == EventType.NodeDataChanged) {
            String requestType = triggerPath.split("/")[2];

            if (Arrays.equals(triggerCode, ZooHelper.Codes.NODE_EXISTS)) {

                if (ZooHelper.exists("/online/" + this.id, zoo))
                    ZooHelper.print("<WARNING> You are already online.");
                else
                    ZooHelper.print("<WARNING> This username has already been taken.");


            } else if (Arrays.equals(triggerCode, ZooHelper.Codes.EXCEPTION))
                ZooHelper.print("<ERROR> It was impossible to " + requestType + " due to unknown/unexpected reasons.");

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
                this.read(ZooHelper.getSender(nodeId), ZooHelper.getMessage(nodeId));

                // after having read the message, delete it and set the watcher for the next one
                zoo.delete(triggerPath + "/" + nodeId, -1);
                zoo.getChildren("/queue/" + this.id, this);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //return;
        }

    }

}