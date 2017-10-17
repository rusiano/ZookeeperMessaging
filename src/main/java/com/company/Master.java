package com.company;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.*;

public class Master {

    private static ZooKeeper zoo;

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {

        // setup connection to Zookeeper
        setupConnection();

        // create tree structure
        createTreeStructure();

    }

    private static void setupConnection() throws IOException, InterruptedException {

        String host = "localhost:2181";
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

    private static void createTreeStructure() throws KeeperException, InterruptedException {

        // create REQUEST node with two children: ENROLL and QUIT
        zoo.create("/request", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/enroll", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/request/quit", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // create REGITRY node as protected: only master has access to users' information
        String auth = "user:pwd";
        zoo.addAuthInfo("digest", auth.getBytes());
        zoo.create("/registry", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

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

