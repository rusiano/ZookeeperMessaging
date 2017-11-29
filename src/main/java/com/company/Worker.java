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
    private boolean isLoginOk, isUsernameOk, isRequestResultProcessed;

    public Worker(ZooKeeper connection, String id) throws IOException, InterruptedException {
        this.id = id;
        this.zoo = connection;
        this.isLoginOk = false;
        this.isUsernameOk = false;
        this.isRequestResultProcessed = false;
    }

    public boolean isLoginOk() {
        return this.isLoginOk;
    }

    public boolean isUsernameOk() {
        return this.isUsernameOk;
    }

    public void setLoginOk(boolean loginOk) {
        this.isLoginOk = loginOk;
    }

    public void setUsernameOk(boolean usernameOk) {
        this.isUsernameOk = usernameOk;
    }

    public String getId() {
        return this.id;
    }

    public ZooKeeper getZoo() {
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

                if (choice.equals(ONLINE_USERS)) {
                    List<String> onlineUsers = getOnlineUsers();
                    if (onlineUsers.size() == 0) {
                        System.out.println(">>> No Users Online! ");
                        break;
                    }

                    System.out.print(">>> Online Users: ");
                    for (String anUser : onlineUsers)
                        System.out.print(anUser + " | ");
                    System.out.println();
                }

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
    public boolean choosesValidReceiver(String idReceiver){

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

    /**
     * Static method that returns the list of users currently online.
     * @return The list containing all the ID's (String) of the online users.
     * @throws KeeperException -
     * @throws InterruptedException -
     * @throws IOException -
     */
    public static List<String> getOnlineUsers() throws KeeperException, InterruptedException, IOException {
        return ZooHelper.getConnection().getChildren("/online", false);

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

        String enrollUserPath = "/request/enroll/" + this.id;

        // create a node (= send a request to the master)
        zoo.create(enrollUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        this.isRequestResultProcessed = false;

        // set a watcher to be triggered when request result is ready
        zoo.getData(enrollUserPath, this, null);

        // wait for either (1) the watcher has finished processing the request result or (2) the timeout to be reached
        long initTime = System.nanoTime();
        do {
            Thread.sleep(10);
        } while (!this.isRequestResultProcessed && (System.nanoTime() - initTime) < ZooHelper.TIMEOUT_IN_NANOSECS);

        byte[] enrollmentCode = ZooHelper.getCode(enrollUserPath, zoo);
        zoo.delete(enrollUserPath, -1);

        boolean timeoutReached = Arrays.equals(enrollmentCode, ZooHelper.Codes.NEW_CHILD);
        if (timeoutReached) ZooHelper.print("<WARNING> Timeout reached because the server is not responding. " +
                "Your request has been deleted.");

        return Arrays.equals(enrollmentCode, ZooHelper.Codes.SUCCESS);
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

        String quitUserPath = "/request/quit/" + this.id;

        // create a node (= send a request to the master)
        zoo.create(quitUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        this.isRequestResultProcessed = false;

        // set a watcher to be triggered when request result is ready
        zoo.getData(quitUserPath, this, null); //Setting watcher if code changes

        // wait for either (1) the watcher has finished processing the request result or (2) the timeout to be reached
        long initTime = System.nanoTime();
        do {
            Thread.sleep(10);
        } while (!this.isRequestResultProcessed && (System.nanoTime() - initTime) < ZooHelper.TIMEOUT_IN_NANOSECS);

        byte[] quitCode = ZooHelper.getCode(quitUserPath, zoo);
        zoo.delete(quitUserPath, -1);

        boolean timeoutReached = Arrays.equals(quitCode, ZooHelper.Codes.NEW_CHILD);
        if (timeoutReached) ZooHelper.print("<WARNING> Timeout reached because the server is not responding. " +
                "Your request has been deleted.");

        return Arrays.equals(quitCode, ZooHelper.Codes.SUCCESS);
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

        // create a node in "/online" to notify (=send a request to) the master
        try {
            zoo.create(onlineUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            this.isRequestResultProcessed = false;
        } catch (KeeperException.NodeExistsException e) {
            ZooHelper.print("<ERROR> Either you are trying to login without being registered or you are already online.");
            return false;
        }

        // set a watcher to be triggered when request result is ready
        zoo.exists(onlineUserPath, this);

        // wait for either (1) the watcher has finished processing the request result or (2) the timeout to be reached
        long initTime = System.nanoTime();
        do {
            Thread.sleep(10);
        } while (!this.isRequestResultProcessed && (System.nanoTime() - initTime) < ZooHelper.TIMEOUT_IN_NANOSECS);

        byte[] onlineCode = ZooHelper.getCode(onlineUserPath, zoo);

        boolean timeoutReached = Arrays.equals(onlineCode, ZooHelper.Codes.NEW_CHILD);
        if (timeoutReached) ZooHelper.print("<WARNING> Timeout reached because the server is not responding. Your request will be deleted.");

        return Arrays.equals(onlineCode, ZooHelper.Codes.SUCCESS);
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

        this.isRequestResultProcessed = false;

        EventType triggerEvent = watchedEvent.getType();
        String triggerPath = watchedEvent.getPath();
        byte[] triggerCode = ZooHelper.getCode(triggerPath, zoo);

        boolean newEnrollmentRequestResult = triggerPath.contains("/enroll") && triggerEvent == EventType.NodeDataChanged;
        boolean newQuitRequestResult = triggerPath.contains("/quit") && triggerEvent == EventType.NodeDataChanged;
        boolean newLoginRequestResult = triggerPath.contains("/online") && triggerEvent == EventType.NodeDataChanged;
        boolean newMessageReceived = triggerPath.contains("/queue/" + this.id) && triggerEvent == EventType.NodeChildrenChanged;

        // NEW ENROLLMENT REQUEST RESULT
        if (newEnrollmentRequestResult) {

            if (Arrays.equals(triggerCode, ZooHelper.Codes.NODE_EXCEPTION)) {

                if (ZooHelper.exists("/online/" + this.id, zoo))
                    ZooHelper.print("<WARNING> You are already online.");
                else
                    ZooHelper.print("<WARNING> This username has already been taken.");


            } else if (Arrays.equals(triggerCode, ZooHelper.Codes.EXCEPTION))
                ZooHelper.print("<ERROR> It was impossible to enroll due to unknown/unexpected reasons.");

        }

        // NEW DEREGISTRATION REQUEST RESULT
        else if (newQuitRequestResult) {

            if (Arrays.equals(triggerCode, ZooHelper.Codes.SUCCESS)) {

                try {
                    zoo.delete(triggerPath.replace("/request/quit", "/online"), -1);
                } catch (KeeperException.NoNodeException ignored) {
                    // if the user was not online there is nothing to delete
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (Arrays.equals(triggerCode, ZooHelper.Codes.EXCEPTION))
                ZooHelper.print("<ERROR> It was impossible to remove your account due to unknown/unexpected reasons.");

        }

        // NEW ONLINE REQUEST RESULT
        else if (newLoginRequestResult) {

            String queueUserPath = triggerPath.replace("/online", "/queue");

            if (Arrays.equals(triggerCode, ZooHelper.Codes.SUCCESS)) {

                // if the request was successful and there are unread messages for the user,
                // the master moved them to the queue: read them
                try {
                    List<String> unreadMessages = zoo.getChildren(queueUserPath, false);
                    if (unreadMessages.size() > 0) {
                        for (String message : unreadMessages) {
                            ZooHelper.print("<INFO> New Unread Message: "
                                    + message.split(":")[1].replaceAll("[0-9]{10}", ""));
                        }
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

                // once read all the old messages, set watcher for possible new incoming messages
                try {
                    zoo.getChildren(queueUserPath, this);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }


            } else {

                // otherwise delete the invalid online user
                try {
                    zoo.delete(triggerPath, -1);
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }

                // finally output the proper warning/error message
                if (Arrays.equals(triggerCode, ZooHelper.Codes.NODE_EXCEPTION))
                    ZooHelper.print("<ERROR> You cannot go online without being registered! Please register first!");
                else
                    ZooHelper.print("<WARNING> Login was rejected for unknown/unexpected reasons");

            }


        }

        // NEW MESSAGE RECEIVED
        else if (newMessageReceived) {
            try {
                String nodeId = zoo.getChildren(triggerPath, false).get(0);
                this.read(ZooHelper.getSender(nodeId), ZooHelper.getMessage(nodeId));
                // after having read the message, delete it and set the watcher for the next one
                zoo.delete(triggerPath + "/" + nodeId, -1);
                zoo.getChildren("/queue/" + this.id, this);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

        }


        this.isRequestResultProcessed = true;

    }


    // When clients wants to leave => the zookeeper instance kills ephemeral nodes and make invalid the session
    public boolean disconnect(){
        try {
            this.zoo.close();
        } catch (Exception e) {
            System.out.println("<ERROR>: trying to disconnect zoo worker. Error: "+ e.getMessage());
            return false;
        }
        return true;
    }

}