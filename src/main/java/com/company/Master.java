package com.company;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.zookeeper.*;

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
        // Only master has access to REGISTRY node
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

        // If non-existing node => warns and exits
        if(zoo.exists(rootPath, false) == null) {
            System.out.println("WARNING: trying to delete a NON-existing node in path " + rootPath);
            return;
        }

        // Look for children of the given path, null if there is no child
        List <String> children = zoo.getChildren(rootPath, false); // False => synchronous process, no watcher to manage sync

        // Node is a leaf iff children == null
        for (String aChildren : children) deleteSubtree(rootPath + '/' + aChildren);

        // when all the children and their subtrees are deleted, delete also the root
        zoo.delete(rootPath, -1);

    }


    /**
     * The method handles the enrollment and deletion requests that are caught by the watcher on "Request" znode.
     * Please remember that requests come in the form of children of the request node (input path);
     * among all the children, the new ones correspond to new requests, i.e. requests to process.
     * @param path The request znode path
     */
    private void manageRequest(String path) throws KeeperException, InterruptedException {


        // Get the last child that has the NEW_CHILD code (newly created -> to process)
        String childPath = null;
        List <String> children = zoo.getChildren(path, null);
        for (String child : children) {
            byte[] child_code = ZooMsg.getNodeCode(zoo, path + '/' + child);
            if (Arrays.equals(child_code,ZooMsg.Codes.NEW_CHILD))// || Arrays.equals(child_code,ZooMsg.Codes.EXCEPTION))
                childPath = path + '/' + child;
        }


        // If child node retrieved is null, there was not new node => removal of previous node
        // !!!!!!!!! ==> ask teacher if there is a way of avoid this trigger(removal) or easier way of getting the child node !!!!!!!!!!!!!!
        if (childPath == null){
            System.out.println("Watcher got from removal or INVALID CODE of a child node in path " + path + " ignoring and setting again the watchers");
            for (String child : children) {
                byte[] child_code = ZooMsg.getNodeCode(zoo, path + '/' + child);
                System.out.println(" - " + path + " with code " + new String (child_code) + " . New node? "+ Arrays.equals(child_code,ZooMsg.Codes.NEW_CHILD));
            }
            return;
        }


        // We continue with the corresponding specification

        // If the child was created or changed(next steps) in '/request/enroll' process action register
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
            System.out.println("Worker properly enroll on " + registryPath);

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
            System.out.println("Worker properly quit on " + childPath);

            // If no exception is raised, change the code of the node to confirm successful request processing
            zoo.setData(childPath, ZooMsg.Codes.SUCCESS, -1);

        }

    }


    /**
     * This void(process) is inherited from Watcher interface. It is fired each time a watcher (that was set in a node)
     * changed or some children were created in the path (depending on how the watcher was set). Ref: https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
     *
     * It contains the asynchronous logic of the master i.e. a request of enrollment from a worker (creation of a child node in the path /request/enroll)
     * @param event Event triggered containing type, path and state
     */
    @Override
    public void process(WatchedEvent event) {

        //System.out.println("Event catch by Master, node from "+ event.getPath() + " full specs: " + event.toString());

        // Watcher triggered in the path '/request' due to child znode changed => manage request(action register or quit)
        if (event.getPath().contains("/request") && event.getType() == Event.EventType.NodeChildrenChanged)
            try { manageRequest(event.getPath()); } catch (Exception e) { e.printStackTrace(); }
        else
            System.out.println("Event type "+ event.getType() +" in path "+ event.getPath() +" NOT EXPECTED");

        // Set the watchers again
        setWatchers();
    }
}