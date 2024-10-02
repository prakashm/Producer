package com.hellokafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerRunnable implements Runnable {

    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final Consumer<String, String> consumer;

    public ConsumerRunnable(String bootstrapServers, String groupId, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;

        // Create Kafka consumer with the required properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto commit

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(this.topic)); // Subscribe to topic
    }

    @Override
    public void run() {
        try {
            boolean processing = false;
            // Poll Kafka in an infinite loop
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    boolean isStart = record.headers().lastHeader("start-marker") != null;
                    boolean isEnd = record.headers().lastHeader("end-marker") != null;

                    // Process each record (custom logic goes here)
                    System.out.printf(
                            "Thread %s consumed record with key %s and value %s from partition %d and offset %d%n",
                            Thread.currentThread().getName(), record.key(), record.value(),
                            record.partition(),
                            record.offset());

                    if (isStart) {
                        System.out.println("Start of message sequence");
                        processing = true;
                    } else if (isEnd) {
                        System.out.println("End of message sequence");
                        processing = false;
                    } else if (processing) {
                        System.out.printf("Processing record: key = %s, value = %s%n", record.key(), record.value());
                    }
                }

                try {
                    // Manually commit offsets after processing the batch
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    System.err.println("Commit failed: " + e);
                }
            }
        } catch (Exception e) {
            System.err.printf("Consumer %s encountered an error: %s%n", Thread.currentThread().getName(),
                    e.getMessage());
        } finally {
            consumer.close();
            System.out.println("Consumer " + Thread.currentThread().getName() + " closed.");
        }
    }
}
