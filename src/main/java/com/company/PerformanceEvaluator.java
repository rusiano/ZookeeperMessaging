package com.company;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PerformanceEvaluator {

    private static final SimpleDateFormat SDF = new SimpleDateFormat("HH:mm:ss.SSS");

    private static final String USER = "user";
    private static final int N_USERS = 10;
    private static final int N_MESSAGES = 1000;
    private static final String LOREM_IPSUM
            = "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit";

    private static Worker[] workers = new Worker[N_USERS];

    public static long totalReceivingTime = 0;
    public static int receivedMessages = 0;

    private static long totalEnrollingTime = 0;
    public static int readMessages = 0;

    public static void main(String args[]) {


        //testEnrollmentSpeed();

        testMessageSpeed(5);


    }

    private static void testEnrollmentSpeed() {

        for (int i = 0; i < N_USERS; i++) {
            try {
                workers[i] = new Worker(ZooHelper.getConnection(), USER+i);

                long initEnrollingTime = new Date().getTime();
                while (!workers[i].enroll()) {
                    initEnrollingTime = new Date().getTime();
                }
                long enrollingTime = new Date().getTime() - initEnrollingTime;
                totalEnrollingTime += enrollingTime;

                while (!workers[i].login()) {}
            } catch (IOException | InterruptedException | KeeperException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Average enrolling time per user: " + totalEnrollingTime / N_USERS + "ms");
        System.out.println("Total enrolling time per " + N_USERS + "users: " + totalEnrollingTime + "ms");

    }

    // Send 1000 messages as fast as possible between two users.
    // Print read messages
    // Print avg time per message
    // Print total elapsed time
    private static void testMessageSpeed(long interval) {

        try {
            Worker w1 = new Worker(ZooHelper.getConnection(), "w1");
            Worker w2 = new Worker(ZooHelper.getConnection(), "w2");

            w1.enroll();
            Thread.sleep(50);
            w1.login();
            Thread.sleep(50);
            w2.enroll();
            Thread.sleep(50);
            w2.login();
            Thread.sleep(50);

            for (int message = 0; message < N_MESSAGES; message++) {
                w1.write(w2.getId(), message + ">" + new Date().getTime() + ">");
                Thread.sleep(interval);
            }

            try {
                System.out.println("===================================================================================");
                System.out.println("Sent messages: " + N_MESSAGES);
                System.out.println("Read Messages: " + readMessages);

                System.out.println("Avg receiving time per message: " + totalReceivingTime / readMessages + "ms");
                System.out.println("Total time for receiving " + readMessages + " messages: " + totalReceivingTime + "ms");
            } catch (ArithmeticException ignored) {
                System.out.println("Aritmetic error. receivedMessages = " + receivedMessages);
            }

            //w1.quit();
            //w2.quit();
        } catch (IOException | KeeperException | InterruptedException e) {
            e.printStackTrace();
        }


    }

}
