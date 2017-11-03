package com.company;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;

public class Master implements Watcher{

    private ZooKeeper zoo;

    Master(String HOST) throws IOException, InterruptedException, KeeperException {
        this.zoo = ZooMsg.setupConnection(HOST);    // Connects to ZooKeeper service
        this.removeTreeStructure();             // Removes previous tree structure
        this.createTreeStructure();             // Creates a new clean tree structure
    }

    /**
     * Removes previous tree structure (if exists).
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private void removeTreeStructure() throws KeeperException, InterruptedException {

        // Give authorization to delete nodes.
        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());

        // !!! REGISTRY and BACKUP do not have to be deleted because they permanently store registered users and unread
        //  messages respectively !!!
        deleteSubtree("/request");
        deleteSubtree("/online");
        deleteSubtree("/queue");
        //deleteSubtree("/registry");
        //deleteSubtree("/backup");

    }

    /**
     * Creates a new clean tree structure.
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private void createTreeStructure() throws KeeperException, InterruptedException {

        // create REQUEST node with two children: ENROLL and QUIT
        zoo.create("/request", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/enroll", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/quit", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // create ONLINE, QUEUE nodes
        zoo.create("/online", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/queue", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // create REGISTRY and BACKUP node as protected: only master has access to users' information and stored messages
        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());
        try {
            zoo.create("/registry", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            zoo.create("/backup", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ignored) { }

        // add all necessary watchers
        this.setWatchers();

    }


    /**
     * The method recursively deletes all the subtree that has as its root the node specified in input.
     * @param rootPath Path of the root node whose subtree has to be deleted
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private void deleteSubtree(String rootPath)  throws KeeperException, InterruptedException {

        // If non-existing node => warns and exits
        if(zoo.exists(rootPath, false) == null) {
            System.out.println("<WARNING> trying to delete a NON-existing node in path " + rootPath + ".");
            return;
        }

        // Look for children of the given path, null if there is no child
        List <String> children = zoo.getChildren(rootPath, false); // False => synchronous process, no watcher to manage sync

        // Node is a leaf iff children == null
        for (String aChildren : children) deleteSubtree(rootPath + '/' + aChildren);

        // when all the children and their subtrees are deleted, delete also the root
        zoo.delete(rootPath, -1);

    }

    /* WATCHERS' METHODS *********************************************************************************************/

    /**
     * This void(process) is inherited from Watcher interface. It is fired each time a watcher is triggered.
     * Ref: https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
     *
     * It contains the asynchronous logic of the master i.e. handling:
     * - a request of enrollment from a worker (creation of a child node in the path /request/enroll)
     * - a request of quitting from a worker (creation of a child node in the path /request/quit)
     * - an online user even (creation of a child node in the path /online)
     * - an offline user event (deletion of a child node in the path /online
     *
     * To distinguish between events of creation and deletion, a search for nodes with NEW_CHILD_CODE is firstly performed.
     * In case of new children, this means that these nodes have been recently added and need some processing. If the code
     * is missing, instead, this clearly means that we have a deletion event.
     *
     * @param event Event triggered containing type, path and state
     */
    @Override
    public void process(WatchedEvent event) {

        String triggerPath = event.getPath();       // the path at which the watcher was triggered
        EventType triggerEvent = event.getType();   // the type of event that triggered the watcher
        String newChild;                            // ID of the last node added to triggerPath; null if no new node is found

        try {

            newChild = getNewChild(triggerPath);

            if (newChild != null && triggerEvent == EventType.NodeChildrenChanged && triggerPath.contains("/request")) {

                // NEW ENROLL/QUIT REQUEST: If the wacher was triggered by one of the children of '/request', then an user tried either to register or to quit.
                if (triggerPath.contains("/enroll"))
                    handleEnrollRequest(newChild);
                else if (triggerPath.contains("/quit"))
                    handleQuitRequest(newChild);

            } else if (newChild != null && triggerEvent == EventType.NodeChildrenChanged && triggerPath.contains("/online")) {

                // NEW ONLINE USER: If the watcher was triggered by '/online' and there is a new child, then a new user tried to go online.
                handleOnlineUser(newChild);

            } else if (newChild == null && triggerEvent == EventType.NodeDeleted && triggerPath.contains("/online/")) {

                // NEW USER OFFLINE: If the event was triggered by a child of /online ("/online/") and a child was deleted, then an user got offline.
                String deletedChild = triggerPath.replace("/online/", "");
                handleOfflineUser(deletedChild);

            } else {

                System.out.println("<WARNING> Watcher got triggered at " + triggerPath + " by unexpected event "
                        + triggerEvent + ". Ignoring and setting new watchers...");

            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Set the watchers again
        this.setWatchers();
    }

    /**
     * Given a path of a znode, the method returns the last children added (last created = first to process).
     * @param triggerPath The path of the znode where the watcher was triggered.
     * @return The last children added.
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private String getNewChild(String triggerPath) throws KeeperException, InterruptedException {

        String newChild = null;

        try {

            List<String> children = zoo.getChildren(triggerPath, null);

            for (String child : children) {
                byte[] child_code = ZooMsg.getNodeCode(zoo, triggerPath + '/' + child);
                if (Arrays.equals(child_code, ZooMsg.Codes.NEW_CHILD))
                    newChild = child;
            }

        } catch (KeeperException.NoNodeException ignored) { }

        return newChild;
    }

    /**
     * The method handles the enrollment requests that are caught by the watcher on "/request/enroll" znode.
     * The worker created a node with his ID in "/enroll". If the user has a valid ID, a new node with the same ID must
     * be created in the "/registry" node and the old enrollment node must be deleted. If the ID is invalid, an exception
     * must be raised by notifying the worker with the appropriate error code.
     *
     * Please remember that among all the children, the new ones correspond to new requests, i.e. requests to process.
     * @param user The ID of the node who requested to enroll
     */
    private void handleEnrollRequest(String user) throws KeeperException, InterruptedException {

        String enrollPath = "/request/enroll/" + user;
        String registryPath = "/registry/" + user;

        try {
            zoo.create(registryPath, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e1) {
            System.out.println("<WARNING> User " + user + " is already registered.");
            zoo.setData(enrollPath, ZooMsg.Codes.NODE_EXISTS, -1);
            return;
        } catch (Exception e1) {
            zoo.setData(enrollPath, ZooMsg.Codes.EXCEPTION, -1);
            return;
        }

        // If no exception is raised, change the code of the node to confirm successful request processing
        zoo.setData(enrollPath, ZooMsg.Codes.SUCCESS, -1);

    }

    /**
     * The method handles the quitting requests that are caught by the watcher on "/request/quit" znode.
     * The worker created a node with his ID in "/quit". The corresponding node in the "/registry" is thus deleted.
     * The worker is then informed about the successful or erroneous deletion with the appropriate code.
     *
     * @param user The ID of the node who requested to quit
     */
    private void handleQuitRequest(String user) throws KeeperException, InterruptedException {

        String quitPath = "/request/quit/" + user;
        String registryPath = "/registry/" + user;

        try {
            zoo.delete(registryPath, -1);
        } catch (KeeperException.NoNodeException e1) {
            zoo.setData(quitPath, ZooMsg.Codes.EXCEPTION, -1);
            return;
        } catch (Exception e1) {
            zoo.setData(quitPath, ZooMsg.Codes.EXCEPTION, -1);
            return;
        }

        // If no exception is raised, change the code of the node to confirm successful request processing
        zoo.setData(quitPath, ZooMsg.Codes.SUCCESS, -1);

    }

    private void handleOnlineUser(String user) throws KeeperException, InterruptedException {

        String registryUserPath = "/registry/" + user;
        String onlineUserPath   = "/online/"   + user;
        String queueUserPath    = "/queue/"    + user;
        String backupUserPath   = "/backup/"   + user;

        // NON-VALID USER: delete the node added by the unregistered user and quit
        if (zoo.exists(registryUserPath, null) == null) {
            System.out.println("<ERROR> " + user + " cannot go online without being registered! Please register yourself first!");
            zoo.delete(onlineUserPath, -1);
            return;
        }

        // VALID USER: add it to /online, then add nodes in /queue and /backup to store his messages
        zoo.setData(onlineUserPath, ZooMsg.Codes.SUCCESS, -1);
        zoo.exists(onlineUserPath, this);    // set watcher to check when it will be deleted

        // create a node for the user inbox and a node for backup (if it's not already there, create it and quit)
        zoo.create(queueUserPath, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        if (zoo.exists(backupUserPath, null) == null) {
            zoo.create(backupUserPath, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            return;
        }

        // If it has backed-up messages, retrieve them and move them to /queue
        List<String> messages = zoo.getChildren(backupUserPath, null);
        for (String message : messages) {
            zoo.create(queueUserPath + "/" + message, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

    }

    private void handleOfflineUser(String user) throws KeeperException, InterruptedException {

        String queueUserPath    = "/queue/"    + user;
        String backupUserPath   = "/backup/"   + user;

        // Get all his unread messages (those still in the queue) and move them to the backup
        List<String> unreadMessages = zoo.getChildren(queueUserPath, null);

        for (String message : unreadMessages) {
            try {
                zoo.create(backupUserPath + "/" + message, ZooMsg.Codes.NEW_CHILD, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) { }

        }

        // Delete the queue
        deleteSubtree(queueUserPath);
    }

    /**
     * Sets up all necessary watchers.
     */
    private void setWatchers(){

        try {

            // Set watchers for new enrolling or quitting requests
            zoo.getChildren("/request/enroll", this);
            zoo.getChildren("/request/quit", this);

            // Set watcher for new online users
            zoo.getChildren("/online", this);

        } catch (Exception e) { e.printStackTrace(); }

    }

}