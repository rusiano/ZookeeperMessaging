package com.company;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZooMsg {

    final static String NEW_CHILD_CODE = "-1";
    final static String EXCEPTION_CODE = "0";
    final static String SUCCESS_CODE = "1";
    final static String NO_NODE_CODE = "2";
    final static String HOST = "localhost:2181";


    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {

        Master master = new Master();

        // Runnable interface (parallel)
        Worker w1 = new Worker(HOST);
        w1.run();

        for(int i = 0; i < 5; i++)
            Thread.sleep(1000);

    }

    static ZooKeeper setupConnection() throws IOException, InterruptedException {

        int sessionTimeout = 3000;
        final CountDownLatch connectionLatch = new CountDownLatch(1);

        //create a connection
        ZooKeeper zoo = new ZooKeeper(HOST, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent we) {

                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }

            }
        });

        connectionLatch.await(10, TimeUnit.SECONDS);

        return zoo;
    }

}
