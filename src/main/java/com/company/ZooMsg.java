package com.company;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZooMsg {

    interface Codes {
        byte[] NEW_CHILD = "-1".getBytes();
        byte[] EXCEPTION = "0".getBytes();
        byte[] SUCCESS = "1".getBytes();
        byte[] NODE_EXISTS = "2".getBytes();
    }


    private final static String HOST = "localhost:2181";


    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {

        String HOST = "localhost:2181";
        //String HOST = "10.10.1.229:2181";

        Master master = new Master(HOST);

        // Runnable interface (parallel)
        Worker w1 = new Worker(HOST, "marcos_test");
        w1.run();

        for(int i = 0; i < 5000; i++)
            Thread.sleep(100);




    }

    static ZooKeeper setupConnection(String HOST) throws IOException, InterruptedException {

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

    /**
     * Given the path of a node, the method return its data. In case of exceptions it returns null.
     * @param path Complete path of the node
     * @return String
     */
    static byte[] getNodeCode(ZooKeeper zoo, String path) {

        byte[] code;
        try {
            code = zoo.getData(path, null, null);
        } catch (Exception e) {
            code = null;
        }

        return code;
    }

}
