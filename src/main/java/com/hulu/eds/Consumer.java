package com.hulu.eds;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by simei.he on 4/21/16.
 */


public class Consumer extends Thread {
    private final KafkaConsumer<String, String> _consumer;
    private final String _topic;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    public Consumer(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        _consumer = new KafkaConsumer<>(props);
        _topic = topic;
    }
    public void shutdown() {
        closed.set(true);
        _consumer.wakeup();

    }
    public void run() {
        try {
            while(true) {

                _consumer.subscribe(Collections.singletonList(_topic));
                ConsumerRecords<String, String> records = _consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("received message: " + record.key() + ", " +
                            record.value() + ", at offset(" + record.offset());
                }
            }
        } catch (WakeupException e) {
            if(!closed.get()) {
                throw e;
            }
        }finally {
            _consumer.close();
        }

    }

}
