## Kafka

This is a simple demo of Kafka.

Both of the producer and consumer adopt a `StringSerializer`, 'cause both of the key and value of the record is of string type. You'll need Avro to deal with much complicated cases.

The consumer plays with auto-commit mechanism, or you could replace it with `seek` instead.


### References
1. [Kafka, Spark, and Avro - Part 1, Kafka 101](http://aseigneurin.github.io/2016/03/02/kafka-spark-avro-kafka-101.html)

2. [Kafka, Spark, and Avro - Part 2, Consuming Kafka messages with Spark](http://aseigneurin.github.io/2016/03/03/kafka-spark-avro-consume-messages-with-spark.html)

3. [Kafka, Spark, and Avro - Part 3, Producing and consuming Avro messages](http://aseigneurin.github.io/2016/03/04/kafka-spark-avro-producing-and-consuming-avro-messages.html)

4. [Apache Kafka for Beginners](http://blog.cloudera.com/blog/2014/09/apache-kafka-for-beginners/)

5. [Running Kafka at Scale](https://engineering.linkedin.com/kafka/running-kafka-scale)
