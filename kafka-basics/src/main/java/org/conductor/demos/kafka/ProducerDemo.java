package org.conductor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("------- I am a Kafka Producer :) -------");

        //connecting insecurely to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", " Message One ");

        //send data
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executed everytime a record is successfully sent of exception is thrown
                if (e == null) {
                    //the record is successfully sent
                    log.info("received new metadata: " + "\n" +
                            "Topic " + recordMetadata.topic() + "\n" +
                            "Partition " + recordMetadata.partition() + "\n" +
                            "Offset " + recordMetadata.offset() + "\n" +
                            "Timestamp " + recordMetadata.timestamp());
                } else {
                    log.error("error while producing", e);
                }
            }
        });

        // flush and close the producer
        producer.flush(); // tells producer to send all data and block until done
        producer.close();
    }
}
