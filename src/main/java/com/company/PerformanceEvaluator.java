package com.company;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class PerformanceEvaluator {

    private static final SimpleDateFormat SDF = new SimpleDateFormat("HH:mm:ss.SSS");

    private static final String USER = "user";
    private static final int N_USERS = 10;
    private static final int N_MESSAGES = 1000;
    private static final String LOREM_IPSUM
            = "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit";

    private static Worker[] workers = new Worker[N_USERS];

    private static long test1Time = 0;
    private static int test1NMessages = 0;
    private static long initElapsedTime;
    private static long endElapsedTime;

    private static long totalEnrollingTime = 0;

    public static void main(String args[]) {


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

        System.out.println("Average");

        // All users registered, we can start sending messages
        test1();


    }

    // Send 1000 messages as fast as possible between two users.
    // Print avg time per message
    private static void test1() {

        for (int message = 0; message < N_MESSAGES; message++) {

            try {
                workers[0].write(workers[1].getId(), LOREM_IPSUM + ">" + SDF.format(new Date()));
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

        }

        System.out.println("Avg receiving time per message: " + test1Time / test1NMessages + "ms");
        System.out.println("Total time for receiving " + N_MESSAGES + " messages: " + test1Time + "ms");
    }

    static void recordElapsedTime(long elapsedTime) {
        test1Time += elapsedTime;
        test1NMessages++;
    }
}
