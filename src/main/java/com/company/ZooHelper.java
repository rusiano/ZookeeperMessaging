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

    private static final SimpleDateFormat SDF = new SimpleDateFormat("HH:mm:ss.SSS");

    interface Codes {
        byte[] EXCEPTION = "-1".getBytes();
        byte[] NEW_CHILD = "0".getBytes();
        byte[] SUCCESS = "1".getBytes();
        byte[] NODE_EXCEPTION = "2".getBytes();
    }

    final static long TIMEOUT_IN_NANOS = (long) (30 * Math.pow(10, 9)); // = 30 secs

    // TODO: change final localhost into a custom ip:port adress (input by the user)
    private static String LOCALHOST = "localhost:2181";

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

    public static ZooKeeper getConnection(String host) throws IOException, InterruptedException {
        if(host == null || host.equals("localhost") || host.equals("local")  || host.equals("") || host.split(":").length < 2)
            host = LOCALHOST;

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
        return nodeId.split(">")[0];
    }

    static String getMessage(String nodeId) {

        String message = nodeId.split(">")[1].replaceAll("[0-9]{10}", "");

        try {
            String stringTime = nodeId.split(">")[2];
            long time = Long.parseLong(stringTime);
            long elapsedTime = new Date().getTime() - time;

            PerformanceEvaluator.totalReceivingTime += elapsedTime;
            PerformanceEvaluator.receivedMessages++;

        } catch (ArrayIndexOutOfBoundsException ignored) {
            // in case we are not testing there is not another field
        }

        return message;
    }

    static String timestamp() {
        return "[" + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + "] ";
    }

    static void print(String message){
        System.out.println(timestamp() + message);
    }

    public static String validateAddressParams(String[] args){
        if(args.length > 1){
            System.out.println(" >> Only need localIP:port parameter");
            System.exit(1);
        }
        else if(args.length == 1)
            return args[0];

        return "localhost";
    }

}
