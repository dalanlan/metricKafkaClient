package com.hulu.eds;

import java.util.concurrent.ExecutionException;

/**
 * Created by simei.he on 4/21/16.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        boolean isAsync = args.length == 0 || !args[0].trim().toLowerCase().equals("sync");
        Producer producerThreadCreate = new Producer(KafkaProperties.TOPIC_CREATE, isAsync);
        producerThreadCreate.start();

        Producer producerThreadInsert = new Producer(KafkaProperties.TOPIC_INSERT, isAsync);
        producerThreadInsert.start();

        System.out.println("Producer done working");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC_CREATE);
        consumerThread.start();
        System.out.println("Consumer done working");

    }
}
