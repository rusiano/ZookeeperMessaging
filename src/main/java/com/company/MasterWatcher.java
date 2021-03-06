package com.company;

import org.apache.zookeeper.*;
import java.util.*;
import org.apache.zookeeper.Watcher.Event.EventType;

public class MasterWatcher implements Watcher {

    private ZooKeeper zoo;
    private ZooHelper zooHelper;

    MasterWatcher(ZooKeeper zoo) {

        this.zoo = zoo;
        this.zooHelper = new ZooHelper(this.zoo);

    }

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

        /* IMPORTANT:
        * after hours and hours of testing I found out that after the last changes, the master is faster
        * in triggering the events than the worker to set the triggers. The result is that the worker's watcher misses the
        * events and it doesn't process them. For this reason (for the moment) it is necessary to make the master watcher
        * sleep for few milliseconds before processing the event to give time to the worker to set its watchers.
        */

        //try { Thread.sleep(350); }
        //catch (InterruptedException e) { e.printStackTrace(); }

        String triggerPath = event.getPath();       // the path at which the watcher was triggered
        EventType triggerEvent = event.getType();   // the type of event that triggered the watcher

        if (triggerEvent != EventType.NodeChildrenChanged)
            return;

        try {

            String newChild = getNewChild(triggerPath); // ID of the last node added to triggerPath; null if no new node is found

            if (triggerPath.contains("/request") && newChild != null) {
                if (triggerPath.contains("/enroll")) handleEnrollRequest(newChild);
                else if (triggerPath.contains("/quit")) handleQuitRequest(newChild);
            }

            else if (triggerPath.contains("/online")) {
                if (newChild != null) handleOnlineUser(newChild);
                else handleOfflineUser();
            }

        } catch (KeeperException e) {
            e.getMessage();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Set the watchers again
        Master.setWatchers();
    }

    /**
     * Given a path of a znode, the method returns the last children added (last created = first to process).
     * @param triggerPath The path of the znode where the watcher was triggered.
     * @return The last children added at the specified path
     */
    private String getNewChild(String triggerPath) throws KeeperException, InterruptedException {

        String newChild = null;
        List<String> children = zoo.getChildren(triggerPath, false);

        for (String child : children) {
            byte[] child_code = zooHelper.getCode(triggerPath + '/' + child);
            if (Arrays.equals(child_code, ZooHelper.Codes.NEW_CHILD)) {
                newChild = child;
                break;
            }
        }

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
     *
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    void handleEnrollRequest(String user) throws KeeperException, InterruptedException {

        String enrollPath = "/request/enroll/" + user;
        String registryPath = "/registry/" + user;

        try {
            zoo.create(registryPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            ZooHelper.print("<INFO> No exceptions. Triggering successful enrollment for " + user);
            zoo.setData(enrollPath, ZooHelper.Codes.SUCCESS, -1);
        } catch (KeeperException.NodeExistsException e1) {
            ZooHelper.print("<WARNING> User " + user + " is already registered. Triggering failed enrollment for " + user);
            zoo.setData(enrollPath, ZooHelper.Codes.NODE_EXCEPTION, -1);
        } catch (Exception e2) {
            ZooHelper.print("<ERROR> Enrollment failed due to unexpected exception " + e2.getMessage());
            zoo.setData(enrollPath, ZooHelper.Codes.EXCEPTION, -1);
        }

    }

    /**
     * The method handles the quitting requests that are caught by the watcher on "/request/quit" znode.
     * The worker created a node with his ID in "/quit". The corresponding node in the "/registry" is thus deleted.
     * The worker is then informed about the successful or erroneous deletion with the appropriate code.
     * @param user The ID of the node who requested to quit
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    void handleQuitRequest(String user) throws KeeperException, InterruptedException {

        String quitPath = "/request/quit/" + user;
        String registryPath = "/registry/" + user;
        String queuePath = "/queue/" + user;
        String backupPath = "/backup/" + user;

        try {
            zoo.delete(registryPath, -1);
            Master.deleteSubtree(queuePath);
            Master.deleteSubtree(backupPath);
        } catch (KeeperException.NoNodeException e1) {
            zoo.setData(quitPath, ZooHelper.Codes.NODE_EXCEPTION, -1);
            return;
        } catch (Exception e1) {
            zoo.setData(quitPath, ZooHelper.Codes.EXCEPTION, -1);
            return;
        }

        // If no exception is raised, change the code of the node to confirm successful request processing
        zoo.setData(quitPath, ZooHelper.Codes.SUCCESS, -1);

    }

    /**
     * The method handles the online enrollments that are caught by the watcher on "/online" znode children.
     * - W: Creates a node with his ID in "/online".
     * - M: Checks that the user was registered. Exiting otherwise.
     * - M: Creates a node in "/queue" and in "/backup" in case the latest was not created previously.
     * - M: If there are messages (znode children) in "/backup/id", the master will be move them to "/queue/id".
     * - M: Once everything is ready, the master updates the znode value to notify the user and set a watcher for offline request
     *
     * @param user The ID of the online node
     *
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private void handleOnlineUser(String user) throws KeeperException, InterruptedException {

        String registryUserPath = "/registry/" + user;
        String onlineUserPath   = "/online/"   + user;
        String queueUserPath    = "/queue/"    + user;
        String backupUserPath   = "/backup/"   + user;

        // NON-VALID USER:
        // Notify worker: change /online code
        if (!zooHelper.exists(registryUserPath)) {
            ZooHelper.print("<ERROR> " + user + " cannot go online without being registered! He must register first!");
            zoo.setData(onlineUserPath, ZooHelper.Codes.NODE_EXCEPTION, -1);
            return;
        }

        // VALID USER:
        // Notify worker: change /online node code and set a watcher to check when it will be deleted
        zoo.setData(onlineUserPath, ZooHelper.Codes.SUCCESS, -1);
        zoo.exists(onlineUserPath, this);    //

        // Create inbox (=queue); warning if there is a previous node (assuming fresh node)
        try {
            zoo.create(queueUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e){
            ZooHelper.print("<WARNING> " + user + " already had an unexpected node in " + queueUserPath + ".");
        }

        // Create/retrieve backup:
        // if there is a node "/backup" and it contains backed-up messages, retrieve them and move them to /queue.
        if (zooHelper.exists(backupUserPath)) {
            List<String> messages = zoo.getChildren(backupUserPath, false);
            for (String message : messages)
                zoo.create(queueUserPath + "/" + message, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            // Then delete the old backed-up messages
            Master.deleteSubtree(backupUserPath);
        }
        // in any case create a fresh /backup/ID znode
        zoo.create(backupUserPath, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);


    }


    /**
     * The method handles the offline enrollments that are caught by the watcher on "/online" znode children.
     * - W: Deletes a node with his ID in "/online".
     * - M: If there are messages (znode children) in "/queue/id", the master will be move them to "/backup/id".
     * - M: Once everything is finish, the master deletes the znode "/queue/id" as well as its children.
    */
    private void handleOfflineUser() throws KeeperException, InterruptedException {

        // First of all, find who is the missing user by comparing online with queue:
        //  queue should differ from online only for the disconnected user
        HashSet<String> currentOnlineUsers = new HashSet<>(zoo.getChildren("/online", false));
        HashSet<String> allOnlineUsers = new HashSet<>(zoo.getChildren("/queue", false));

        // remove from the original set of all the online users, the users that are still online
        allOnlineUsers.removeAll(currentOnlineUsers);

        // in the resulting list now we have only the disconnected (missing) users
        for (String missingUser : allOnlineUsers) {
            ZooHelper.print("<INFO> " + missingUser + " disconnected.");

            String queueUserPath    = "/queue/"    + missingUser;
            String backupUserPath   = "/backup/"   + missingUser;

            // Get all his unread messages (those still in the queue) and move them to the backup
            List<String> unreadMessages = zoo.getChildren(queueUserPath, false);
            for (String message : unreadMessages) {
                try {
                    zoo.create(backupUserPath + "/" + message, ZooHelper.Codes.NEW_CHILD, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException ignored) { }

            }

            // Once it's done, delete the queue
            Master.deleteSubtree(queueUserPath);

        }

    }
}
