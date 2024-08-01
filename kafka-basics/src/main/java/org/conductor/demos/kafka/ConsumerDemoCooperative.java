package org.conductor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());


    public static void main(String[] args) {
        String groupId = "my-java-app";
        String topic = "demo_java";

        log.info("------- I am a Kafka Consumer :) -------");

        //connecting insecurely to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        //To allows consumers in a group to resume at the right offset, I need to set group.id
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); // none/earliest/latest
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        //properties.setProperty("group.instance.id", "..."); // stratergy for static consumers


        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a ref to teh main thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a Shutdown, lets exit by calling consumer.wakeup()...");
                consumer.wakeup();
                //join the mainThread to allow the execution of the code in the mainThread
                try {
                    mainThread.join();
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        });

        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {
                log.info("Polling");
                //this line will throw wakeup exception
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
                //auto offset commit behaviour - auto.commit.interval.ms = 5000 - offset commit happens every 5 sec

            }
        } catch (WakeupException we) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.info("Unexpected exception in the consumer");
        } finally {
            consumer.close(); //close the consumer & this will also commit the offset
            // kafka does some internal closure steps
            // rebalance occurs when new consumer joins the group and when new partition get added to topic
            log.info("Consumer is now gracefully shutdown !!");
        }

    }

}
