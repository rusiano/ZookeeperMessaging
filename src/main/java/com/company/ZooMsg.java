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
        byte[] EXCEPTION = "-1".getBytes();
        byte[] NEW_CHILD = "0".getBytes();
        byte[] SUCCESS = "1".getBytes();
        byte[] NODE_EXISTS = "2".getBytes();
    }


    private final static String HOST = "localhost:2181";
    //String HOST = "10.10.1.229:2181";

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {

        Master master = new Master(HOST);

        // Runnable interface (parallel)
        Worker w1 = new Worker(HOST, "marcos");
        Worker max = new Worker(HOST, "max");

        w1.enroll();
        w1.goOnline();

        max.enroll();
        max.goOnline();

        w1.write("max", "Ey!");
        w1.write("max", "Ey2!");
        w1.write("max", "Ey3!");
        w1.write("max", "Bye!");

        max.read();
        max.readAll();

        w1.goOffline();
        max.goOffline();

        w1.quit();
        max.quit();

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
