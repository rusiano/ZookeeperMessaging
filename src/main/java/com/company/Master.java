package com.company;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.zookeeper.*;

public class Master implements Watcher{

    private ZooKeeper zoo;

    Master() throws IOException, InterruptedException, KeeperException {
        this.zoo = ZooMsg.setupConnection();    // Connects to ZooKeeper service
        this.removeTreeStructure();             // Removes previous tree structure
        this.createTreeStructure();             // Creates a new clean tree structure
    }

    /**
     * Removes previous tree structure (if exists).
     * @throws KeeperException -
     * @throws InterruptedException -
     */
    private void removeTreeStructure() throws KeeperException, InterruptedException {
        // create REGITRY node as protected: only master has access to users' information
        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());
        deleteSubtree("/request");
        deleteSubtree("/registry");
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
     * @throws KeeperException -
     * @throws InterruptedException -
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
     * The method handles the enrollment and deletion requests that are caught by the watcher on "Request" znode.
     * Please remember that requests come in the form of children of the request node (input path);
     * among all the children, the new ones correspond to new requests, i.e. requests to process.
     * @param path The request znode path
     */
    private void manageRequest(String path) throws KeeperException, InterruptedException {


        // Get the first child that has the NEW_CHILD code (newly created -> to process)
        String childPath = "";
        List <String> children = zoo.getChildren(path, null);
        for (String child : children) {
            childPath = path + '/' + child;
            if (Arrays.equals(ZooMsg.Codes.NEW_CHILD, ZooMsg.getNodeCode(zoo, childPath)))
                break;
        }


        // If child node retrieved is not the new node, notify error and return
        if (!Arrays.equals(ZooMsg.Codes.NEW_CHILD, ZooMsg.getNodeCode(zoo, childPath))){
            System.out.println("ERROR Manager Request:No valid node in" + path);
            return;
        }


        // We continue with the corresponding specification

        // If the child was created or changed in '/request/enroll' process action register
        if (childPath.contains("enroll")) {

            // Change requestPath (/request/enroll/idXXX) into registryPath (/registry/idXXX)
            String registryPath = childPath.replace("request/enroll", "registry");

            // Try to register the user: catch exceptions for already registered users
            try {
                zoo.create(registryPath, "0".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e1) {
                zoo.setData(childPath, ZooMsg.Codes.NODE_EXISTS, -1);
                return;
            } catch (Exception e1) {
                zoo.setData(childPath, ZooMsg.Codes.EXCEPTION, -1);
                return;
            }

            // If no exception is raised, change the code of the node to confirm successful request processing
            zoo.setData(childPath, ZooMsg.Codes.SUCCESS, -1);

        // If the child was created or changed in '/request/quit' process action quit
        } else if (childPath.contains("quit")) {

            String new_path = childPath.replace("request/quit", "registry");

            try {
                zoo.delete(new_path, -1);
            } catch (KeeperException.NoNodeException e1) {
                zoo.setData(childPath, ZooMsg.Codes.NODE_EXISTS, -1);
                return;
            } catch (Exception e1) {
                zoo.setData(childPath, ZooMsg.Codes.EXCEPTION, -1);
                return;
            }

            // If no exception is raised, change the code of the node to confirm successful request processing
            zoo.setData(childPath, ZooMsg.Codes.SUCCESS, -1);

        }

        // Set again watcher
        this.setWatchers();

    }


    /**
     * The method handles the enrollment and deletion requests that are caught by the watcher on "Request" znode.
     * Please remember that requests come in the form of children of the request node (input path);
     * among all the children, the new ones correspond to new requests, i.e. requests to process.
     * @param event The instance of the watchedevent including status, type and path
     */
    @Override
    public void process(WatchedEvent event) {

        //ZooMsg.out.println("Event catch by Master, node from "+ event.getPath() + " full specs: " + event.toString());

        // Watcher triggered in the path '/request' due to child znode changed => manage request(action register or quit)
        if (event.getPath().contains("/request") && event.getType() == Event.EventType.NodeChildrenChanged)
            try { manageRequest(event.getPath()); } catch (Exception e) { e.printStackTrace(); }
        else
            System.out.println("Event type "+ event.getType() +" in path "+ event.getPath() +" NOT EXPECTED");
    }
}