package com.company;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class ZooHelper {

    interface Codes {
        byte[] EXCEPTION = "-1".getBytes();
        byte[] NEW_CHILD = "0".getBytes();
        byte[] SUCCESS = "1".getBytes();
        byte[] NODE_EXISTS = "2".getBytes();
    }

    private final static String LOCALHOST = "localhost:2181";

    private static ZooKeeper getConnection(String host) throws IOException, InterruptedException {
        int sessionTimeout = 3000;
        final CountDownLatch connectionLatch = new CountDownLatch(1);

        // Create a connection with the given host
        ZooKeeper zoo = new ZooKeeper(host, sessionTimeout, new Watcher() {

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

    static ZooKeeper getConnection() throws IOException, InterruptedException {
        return getConnection(LOCALHOST);
    }


    static byte[] getCode(String path, ZooKeeper zoo) {

        if (path == null || path.equals("") || !exists(path, zoo))
            return null;

        try {
            return zoo.getData(path, null, null);
        } catch (KeeperException e) {
            e.printStackTrace();
            return null;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }

    }

    static String getSender(String nodeId) {
        return nodeId.split(":")[0];
    }

    static String getMessage(String nodeId) {
        return nodeId.split(":")[1].replaceAll("[0-9]{10}", "");
    }

    static boolean exists(String path, ZooKeeper zoo) {
        try {
            return zoo.exists(path, null) != null;
        } catch (KeeperException.NoNodeException e) {
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    static String timestamp() {
        return "[" + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + "] ";
    }

    static void print(String message){
        System.out.println(timestamp() + message);
    }

}