package com.company;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;


public class Master implements Watcher{

    private ZooKeeper zoo;
    private String host;

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {

        Master master = new Master();

        // setup connection to Zookeeper
        master.setupConnection();

        // create tree structure
        master.removeTreeStructure(); //For fresh initialization
        master.createTreeStructure();


        master.addWatchers();

        // Runnable interface (parallel)
            Worker w1 = new Worker(master.host);
            w1.run();

        for(int i = 0; i < 5; i++)
            Thread.sleep(1000);

    }

    private void setupConnection() throws IOException, InterruptedException {

        this.host = "localhost:2181";
        int sessionTimeout = 3000;
        final CountDownLatch connectionLatch = new CountDownLatch(1);

        //create a connection
        zoo = new ZooKeeper(host, sessionTimeout , new Watcher() {

            @Override
            public void process(WatchedEvent we) {

                if(we.getState() == Event.KeeperState.SyncConnected){
                    connectionLatch.countDown();
                }

            }
        });

        connectionLatch.await(10, TimeUnit.SECONDS);

    }

    private void createTreeStructure() throws KeeperException, InterruptedException {

        // create REQUEST node with two children: ENROLL and QUIT
        zoo.create("/request", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/enroll", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/quit", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // create REGITRY node as protected: only master has access to users' information
        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());
        zoo.create("/registry", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

    }

    private void recursiveDelete(String path)  throws KeeperException, InterruptedException {
        if((zoo.exists(path, null) == (Stat)null))
            return;

        List <String> children = zoo.getChildren(path, false);

        if(children == null || children.isEmpty()) {
            zoo.delete(path, -1);
            return;
        }

        for(Iterator<String> iterator = children.iterator(); iterator.hasNext();)
            recursiveDelete(path + '/' +iterator.next());

        zoo.delete(path, -1);
    }

    private void removeTreeStructure() throws KeeperException, InterruptedException {
        // create REGITRY node as protected: only master has access to users' information
        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());;
        recursiveDelete("/request");
        recursiveDelete("/registry");
    }

    private void addWatchers(){
        try {

            //Set watchers on request enroll and quit
            //BasicWatcher watcher = new BasicWatcher();
            zoo.getChildren("/request/enroll", this);
            zoo.getChildren("/request/quit", this);

        } catch (Exception e) { e.printStackTrace(); }
    }

    //Return the value of the node of the specified path, if any error occurrs then returns null
    private String getData(String path) {
        String value = null;
        try {
            value = new String(zoo.getData(path, null, null), "UTF-8");
        } catch (Exception e) {
            value = null;
        }

        return value;
    }

    public void manageRequest(String path){
        try {

            //Request are considered child nodes, we need to get the new nodes
            String child_path = "";
            List <String> children = zoo.getChildren(path, null);
            for(Iterator<String> iterator = children.iterator(); iterator.hasNext();){
                child_path = iterator.next();
                if("-1".getBytes().equals(getData(path)))
                    break;
            }
            path = path + '/'+ child_path;

            //We check if the child node retrieved was indeed the new one ("-1" value)
            //If so we continue with the specification
            if ("-1".equals(getData(path))){
                if (path.contains("enroll")) {
                    String new_path = path.replace("request/enroll", "registry");
                    try {
                        zoo.create(new_path, "0".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e1) {
                        zoo.setData(path, "2".getBytes(), -1);
                        return;
                    } catch (Exception e1) {
                        zoo.setData(path, "0".getBytes(), -1);
                        return;
                    }
                    zoo.setData(path, "1".getBytes(), -1);
                } else if (path.contains("quit")) {
                    String new_path = path.replace("request/quit", "registry");
                    try {
                        zoo.delete(new_path, -1);
                    } catch (KeeperException.NoNodeException e1) {
                        zoo.setData(path, "2".getBytes(), -1);
                        return;
                    } catch (Exception e1) {
                        zoo.setData(path, "0".getBytes(), -1);
                        return;
                    }
                    zoo.setData(path, "1".getBytes(), -1);
                }
            }
            else
                System.out.println("Something went wrong with watcher data manipulation on Master");

            //Set again watcher
            addWatchers();

        } catch (Exception e) { e.printStackTrace(); }
    }

    @Override
    public void process(WatchedEvent event) {

        //System.out.println("Evento cogido por el Master"+ event.getPath() + " " + event.toString());

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

