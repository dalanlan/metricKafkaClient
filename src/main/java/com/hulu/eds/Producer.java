package com.hulu.eds;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Properties;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

/**
 * Created by simei.he on 4/21/16.
 */
public class Producer extends Thread {
    private final KafkaProducer<String, String> _producer;
    private final String _topic;
    private final Boolean _isAsyc;
    private final ColumnPool _pool;

    public Producer(String topic, Boolean isAsyc) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", KafkaProperties.PRODUCER_CLIENT_ID);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        _producer = new KafkaProducer<>(props);
        _topic = topic;
        _isAsyc = isAsyc;
        _pool = new ColumnPool();
    }

    // input: tableIndex
    // output: SQL
    private String actionTable(int tableIndex) {
        if(_topic == KafkaProperties.TOPIC_CREATE) {
            return createTable(tableIndex);
        }
        if (_topic == KafkaProperties.TOPIC_INSERT) {
            return insertTable(tableIndex, createMovieConfig(tableIndex));
        }
        if (_topic == KafkaProperties.TOPIC_ALTER) {
            return alterTable(tableIndex);
        }
        return "";
    }
    private String createTable(int tableIndex) {
        StringBuilder _tableName = new StringBuilder("fakeTable").append(Integer.toString(tableIndex));

        String _sql = String.format("CREATE TABLE %s ("+
        "TMSId VARCHAR(45) NOT NULL, "+
        "title VARCHAR(50) NOT NULL, "+
        "description text NOT NULL, "+
        "PRIMARY KEY(TMSId) );", _tableName.toString());

        return _sql;

    }

    private String alterTable(int tableIndex) {
        StringBuilder tableName = new StringBuilder("fakeTable").append(Integer.toString(tableIndex));

        Pair column = _pool.consumePool();

        String columnName = column._columnName;
        String columnType = column._columnType;

        String sql = String.format("ALTER TABLE %s "+
        "ADD %s %s;",tableName.toString(), columnName, columnType);
        return sql;
    }

    private String insertTable(int tableIndex, Map<String, String> config) {
        StringBuilder tableName = new StringBuilder("fakeTable").append(Integer.toString(tableIndex));

        String sql = String.format("INSERT INTO %s "+
        "( TMSId, title, description) "+
        "VALUES (%s, %s, %s);", tableName.toString(), config.get("TMSId"), config.get("title"), config.get("description"));
        return sql;

    }

    private Map<String, String> createMovieConfig(int tableIndex) {
        Map<String, String> movieConfig = new HashMap<>();
        movieConfig.put("TMSId", Integer.toString(tableIndex));
        movieConfig.put("title", "title"+Integer.toString(tableIndex));
        movieConfig.put("description", "desc"+Integer.toString(tableIndex));

        return movieConfig;
    }

    public void run() {
        for(int tableInd = 1; tableInd < 10; tableInd++) {
            // send async
            if(_isAsyc) {

                long startTime = System.currentTimeMillis();
                _producer.send(new ProducerRecord<String, String>(_topic, actionTable(tableInd)),
                        new DemoCallBack(startTime, actionTable(tableInd)));
            } else {
                try {
                    _producer.send(new ProducerRecord<String, String>(_topic, actionTable(tableInd))).get();
                }
                catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}

class ColumnPool {
    public int _offset;
    public ArrayList<Pair> _array;
    public final int _capacity;

    public ColumnPool() {
        _offset = 0;

        _array = new ArrayList<>();

        for(char ch = 'A'; ch < 'K'; ch++) {
            _array.add(new Pair(Character.toString(ch), "INT"));
        }
        _capacity = _array.size();
    }

    public Pair consumePool() {
        if(_offset < _capacity) {
            return _array.get(_offset++);
        }
        else {
            System.out.println("Error: index out of range.");
            return null;
        }

    }
}

class Pair {
    public String _columnName;
    public String _columnType;
    Pair(String name, String type) {
        _columnName = name;
        _columnType = type;
    }
}

class DemoCallBack implements Callback {
    private final long _startTime;
    private final String _message;

    public DemoCallBack(long startTime, String message) {
        _startTime = startTime;
        _message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - _startTime;
        if(metadata != null) {
            System.out.println(
                    "record " + _message + ", sent to partition(" + metadata.partition() + "), offset(" +
                            metadata.offset() + ") in "+ elapsedTime + " ms");

        }
        else {
            exception.printStackTrace();
        }
    }
}