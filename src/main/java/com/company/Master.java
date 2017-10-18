package com.company;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;


public class Master implements Watcher{

    private ZooKeeper zoo;

    Master() throws IOException, InterruptedException, KeeperException {
        this.zoo = ZooMsg.setupConnection();    // Connects to ZooKeeper service
        this.removeTreeStructure();             // Removes previous tree structure
        this.createTreeStructure();             // Creates a new clean tree structure
    }

    /**
     * Removes previous tree structure (if exists).
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void removeTreeStructure() throws KeeperException, InterruptedException {
        // create REGITRY node as protected: only master has access to users' information
        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());;
        deleteSubtree("/request");
        deleteSubtree("/registry");
    }

    /**
     * Creates a new clean tree structure.
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void createTreeStructure() throws KeeperException, InterruptedException {

        // create REQUEST node with two children: ENROLL and QUIT
        zoo.create("/request", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/enroll", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/quit", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // create REGITRY node as protected: only master has access to users' information
        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());
        zoo.create("/registry", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

        // add all necessary watchers
        this.setWatchers();

    }

    /**
     * Sets up all necessary watchers.
     */
    private void setWatchers(){

        try {

            //Set watchers on request enroll and quit
            zoo.getChildren("/request/enroll", this);
            zoo.getChildren("/request/quit", this);

        } catch (Exception e) { e.printStackTrace(); }

    }


    /**
     * The method recursively deletes all the subtree that has as its root the node specified in input.
     * @param rootPath Path of the root node whose subtree has to be deleted
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void deleteSubtree(String rootPath)  throws KeeperException, InterruptedException {

        // if the path is invalid: do nothing
        if ((zoo.exists(rootPath, null) == null))
            return;

        List <String> children = zoo.getChildren(rootPath, false);

        // if the node is a leaf: remove the node and stop
        if (children == null || children.isEmpty()) {
            zoo.delete(rootPath, -1);
            return;
        }

        // otherwise iterate over the children and recursively delete all their subtrees
        for (String aChildren : children) deleteSubtree(rootPath + '/' + aChildren);

        // when all the childrens and their subtrees are deleted, delete also the root
        zoo.delete(rootPath, -1);

    }

    /**
     * Given the path of a node, the method return its data. In case of exceptions it returns null.
     * @param path Complete path of the node
     * @return String
     */
    private String getNodeData(String path) {

        String value;
        try {
            value = new String(zoo.getData(path, null, null), "UTF-8");
        } catch (Exception e) {
            value = null;
        }

        return value;
    }

    /**
     * The method handles the enrollment and deletion requests that are caught by the watcher on "Request" znode.
     * Please remember that requests come in the form of children of the request node (input path);
     * among all the children, the new ones correspond to new requests, i.e. requests to process.
     * @param path The request znode path
     */
    private void manageRequest(String path){

        try {

            // Get the first child that has the NEW_CHILD_CODE (newly created -> to process)
            String childPath = "";
            List <String> children = zoo.getChildren(path, null);
            for (String child : children) {
                childPath = path + '/' + child;
                if (ZooMsg.NEW_CHILD_CODE.equals(this.getNodeData(childPath)))
                    break;
            }

            //We check if the child node retrieved was indeed the new one ("-1" value):
            // if so we continue with the corresponding specification
            if (ZooMsg.NEW_CHILD_CODE.equals(this.getNodeData(childPath))){

                if (childPath.contains("enroll")) {

                    // Change requestPath (/request/enroll/idXXX) into registryPath (/registry/idXXX)
                    String registryPath = childPath.replace("request/enroll", "registry");

                    // Try to register the user: catch exceptions for already registered users
                    try {
                        zoo.create(registryPath, "0".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e1) {
                        zoo.setData(childPath, ZooMsg.NO_NODE_CODE.getBytes(), -1);
                        return;
                    } catch (Exception e1) {
                        zoo.setData(childPath, ZooMsg.EXCEPTION_CODE.getBytes(), -1);
                        return;
                    }

                    // If no exception is raised, change the code of the node to confirm successful request processing
                    zoo.setData(childPath, ZooMsg.SUCCESS_CODE.getBytes(), -1);

                } else if (childPath.contains("quit")) {

                    String new_path = childPath.replace("request/quit", "registry");

                    try {
                        zoo.delete(new_path, -1);
                    } catch (KeeperException.NoNodeException e1) {
                        zoo.setData(childPath, ZooMsg.NO_NODE_CODE.getBytes(), -1);
                        return;
                    } catch (Exception e1) {
                        zoo.setData(childPath, ZooMsg.EXCEPTION_CODE.getBytes(), -1);
                        return;
                    }

                    // If no exception is raised, change the code of the node to confirm successful request processing
                    zoo.setData(childPath, ZooMsg.SUCCESS_CODE.getBytes(), -1);

                }
            }
            else
                System.out.println("Something went wrong with watcher data manipulation on Master");

            // Set again watcher
            this.setWatchers();

        } catch (Exception e) { e.printStackTrace(); }

    }

    @Override
    public void process(WatchedEvent event) {

        //ZooMsg.out.println("Evento cogido por el Master"+ event.getPath() + " " + event.toString());

        if (event.getType() == Event.EventType.NodeChildrenChanged)
            if(event.getPath().contains("/request"))
                manageRequest(event.getPath());
            else
                System.out.println("Created node"+ event.getPath() +"NOT EXPECTED");
    }


}


/** Valerio's code *********************************************************************************************
 //create znode
 zoo.create("/test", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
 //create znode sequential
 zoo.create("/test/sequential", "znode_sequential".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
 //create znode ephemereal
 zoo.create("/test/ephemeral", "znode_ephemeral".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

 String auth = "user:pwd";
 zoo.addAuthInfo("digest", auth.getBytes());
 //create protected node
 zoo.create("/test/protected", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

 //set a watcher
 BasicWatcher w = new BasicWatcher();
 //zoo.getData("/test/ephemeral", w, null);

 //fire the watcher
 //zoo.setData("/test/ephemeral", "changed value".getBytes(), -1);

 //delete protected node
 //zoo.delete("/test/protected", -1);
 ***************************************************************************************************************/

