package com.hulu.eds;

/**
 * Created by simei.he on 4/21/16.
 */


public class KafkaProperties {
    public static final String TOPIC_CREATE = "create";
    public static final String TOPIC_ALTER = "alter";
    public static final String TOPIC_INSERT = "insert";


    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;

    public static final String CONSUMER_CLIENT_ID = "ConsumerDemo";
    public static final String PRODUCER_CLIENT_ID = "ProducerDemo";

    private KafkaProperties() {}
}