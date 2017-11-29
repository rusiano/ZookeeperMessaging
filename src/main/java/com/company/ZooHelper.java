package com.company;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZooHelper {

    interface Codes {
        byte[] EXCEPTION = "-1".getBytes();
        byte[] NEW_CHILD = "0".getBytes();
        byte[] SUCCESS = "1".getBytes();
        byte[] NODE_EXCEPTION = "2".getBytes();
    }

    final static long TIMEOUT_IN_NANOS = (long) (30 * Math.pow(10, 9)); // = 30 secs

    // TODO: change final localhost into a custom ip:port adress (input by the user)
    private final static String LOCALHOST = "localhost:2181";

    private ZooKeeper zoo;

    public ZooHelper() {
        try {
            this.zoo = getConnection(LOCALHOST);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ZooHelper(ZooKeeper zoo) {
        this.zoo = zoo;
    }

    private static ZooKeeper getConnection(String host) throws IOException, InterruptedException {
        int sessionTimeout = 3000;
        final CountDownLatch connectionLatch = new CountDownLatch(1);

        // Create a connection with the given host
        ZooKeeper zoo = new ZooKeeper(host, sessionTimeout, we -> {

            if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
            }

        });
        connectionLatch.await(1, TimeUnit.SECONDS);

        return zoo;
    }

    public static ZooKeeper getConnection() throws IOException, InterruptedException {
        return getConnection(LOCALHOST);
    }


    byte[] getCode(String path) {

        if (path == null || path.equals("") || !exists(path))
            return null;

        try {
            return zoo.getData(path, null, null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }

    }

    boolean exists(String path) {
        try {
            return zoo.exists(path, null) != null;
        } catch (Exception e) {
            return false;
        }
    }

    static String getSender(String nodeId) {
        return nodeId.split(":")[0];
    }

    static String getMessage(String nodeId) {
        return nodeId.split(":")[1].replaceAll("[0-9]{10}", "");
    }

    static String timestamp() {
        return "[" + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + "] ";
    }

    static void print(String message){
        System.out.println(timestamp() + message);
    }

    
}
