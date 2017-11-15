package com.company;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import org.apache.zookeeper.*;

class Master{

    private static Scanner input = new Scanner(System.in);

    private static ZooKeeper zoo;
    private static Watcher watcher;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        zoo = ZooHelper.getConnection();    // Connects to ZooKeeper service
        if (zoo == null) { System.out.println("zoo is null"); return;}

        watcher = new MasterWatcher(zoo);   //  Sets the master watcher

/*
        System.out.print("> Do you want to completely remove the previous tree structure (Y/N)? ");
        String answer = input.nextLine().toUpperCase();
        if (answer.equals("Y"))
            removeTreeStructure(true);      // Removes previous tree structure
        else if (answer.equals("N"))
            removeTreeStructure(false);
        else
            System.out.println("<ERROR> " + answer + " is not a valid command. Please retry.");
*/

        removeTreeStructure(true);      // Removes previous tree structure
        createTreeStructure();             // Creates a new clean tree structure

        System.out.println("> Enter any key at any time to stop the master.");
        System.out.println("> Master is running...");
        input.nextLine();
        System.out.println("> Stopping the master...");
    }

    /**
     * Removes previous tree structure (if exists).
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private static void removeTreeStructure(boolean all) throws KeeperException, InterruptedException {

        // Give authorization to delete nodes.
        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());

        // !!! REGISTRY and BACKUP do not have to be deleted because they permanently store registered users and unread
        //  messages respectively !!!
        deleteSubtree("/request");
        deleteSubtree("/online");
        deleteSubtree("/queue");
        if (all) {
            deleteSubtree("/registry");
            deleteSubtree("/backup");
        }

    }

    /**
     * Creates a new clean tree structure.
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private static void createTreeStructure() throws KeeperException, InterruptedException {

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
        setWatchers();

    }


    /**
     * The method recursively deletes all the subtree that has as its root the node specified in input.
     * @param rootPath Path of the root node whose subtree has to be deleted
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    static void deleteSubtree(String rootPath)  throws KeeperException, InterruptedException {

        // If non-existing node => warns and exits
        if(zoo.exists(rootPath, false) == null) {
            ZooHelper.print("<WARNING> trying to delete a NON-existing node in path " + rootPath + ".");
            return;
        }

        // Look for children of the given path, null if there is no child
        List<String> children = zoo.getChildren(rootPath, false); // False => synchronous process, no watcher to manage sync

        // Node is a leaf iff children == null
        for (String aChildren : children) deleteSubtree(rootPath + '/' + aChildren);

        // when all the children and their subtrees are deleted, delete also the root
        zoo.delete(rootPath, -1);

    }

    static void getOnlineUsers() throws KeeperException, InterruptedException, IOException {
        List<String> onlineIds = ZooHelper.getConnection().getChildren("/online", false);

        if (onlineIds.size() == 0){
            System.out.println("[ No Online Users ]");
            return;
        }

        System.out.print("[ Online Users: ");
        for (String id : onlineIds) {
            System.out.print(id + " ");
        }
        System.out.println("]");
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

}