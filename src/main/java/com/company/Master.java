package com.company;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import org.apache.zookeeper.*;

class Master{

    private static final String YES = "Y";
    private static final String NO = "N";
    private static Scanner input = new Scanner(System.in);

    private static ZooKeeper zoo;
    private static ZooHelper zooHelper;
    private static MasterWatcher watcher;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        zoo = ZooHelper.getConnection();    // Connects to ZooKeeper service
        if (zoo == null) { ZooHelper.print("<ERROR> It was impossible to establish a connection to Zookeeper. Exiting..."); return;}

        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());

        zooHelper = new ZooHelper(zoo);     // Instantiate a helper
        watcher = new MasterWatcher(zoo);   //  Sets the master watcher

        do {
            System.out.print("> Do you want to completely remove the previous tree structure (Y/N)? ");
            String answer = input.nextLine().toUpperCase();

            if (answer.equals(YES)) {
                removeTreeStructure();      // Removes previous tree structure
                createTreeStructure();
                setWatchers();
                break;
            } else if (answer.equals(NO)) {
                manageUnprocessedRequests();
                setWatchers();
                break;
            }

            System.out.println("<ERROR> " + answer + " is not a valid command. Please retry.");

        } while (true);

        System.out.println("<<< ENTER ANY KEY AT ANY TIME TO STOP THE MASTER. >>>");
        System.out.println("> Master is running...");
        input.nextLine();
        System.out.println("> Stopping the master...");
    }

    /**
     * Removes previous tree structure (if exists).
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private static void removeTreeStructure() throws KeeperException, InterruptedException {

        // !!! REGISTRY and BACKUP do not have to be deleted because they permanently store registered users and unread
        //  messages respectively !!!
        deleteSubtree("/request");
        deleteSubtree("/online");
        deleteSubtree("/queue");
        deleteSubtree("/registry");
        deleteSubtree("/backup");

    }

    /**
     * Creates a new clean tree structure.
     */
    private static void createTreeStructure() throws KeeperException, InterruptedException {

        // create REQUEST node with two children: ENROLL and QUIT
        zoo.create("/request", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/enroll", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/quit", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // create ONLINE, QUEUE nodes
        zoo.create("/online", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/queue", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        try {
            zoo.create("/registry", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            zoo.create("/backup", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ignored) { }

    }


    /**
     * The method recursively deletes all the subtree that has as its root the node specified in input.
     * @param rootPath Path of the root node whose subtree has to be deleted
     */
    static void deleteSubtree(String rootPath) throws KeeperException, InterruptedException {

        // If non-existing node => warns and exits
        if(!zooHelper.exists(rootPath)) {
            ZooHelper.print("<WARNING> trying to delete a NON-existing node in path " + rootPath + ".");
            return;
        }

        // Look for children of the given path, null if there is no child
        List<String> children = zoo.getChildren(rootPath, false);

        // Node is a leaf iff children == null
        for (String aChildren : children) deleteSubtree(rootPath + '/' + aChildren);

        // when all the children and their subtrees are deleted, delete also the root
        zoo.delete(rootPath, -1);

    }

    /**
     * Sets up all necessary watchers.
     */
    static void setWatchers(){

        try {

            // Set watchers for new enrolling or quitting requests
            zoo.getChildren("/request/enroll", watcher);
            zoo.getChildren("/request/quit", watcher);

            // Set watcher for new online users
            zoo.getChildren("/online", watcher);

        } catch (Exception e) { e.printStackTrace(); }

    }

    /**
     * Checks for any unprocessed requests sent by workers and forwards them to the watcher.
     */

    private static void manageUnprocessedRequests() throws KeeperException, InterruptedException {

        // handle quit requests first
        List<String> unprocessedQuittingIds = zoo.getChildren("/request/quit", false,null);
        for (String id : unprocessedQuittingIds) watcher.handleQuitRequest(id);

        // handle enrollment requests
        List<String> unprocessedEnrollingIds = zoo.getChildren("/request/enroll", false,null);
        for (String id : unprocessedEnrollingIds) watcher.handleEnrollRequest(id);

    }

}