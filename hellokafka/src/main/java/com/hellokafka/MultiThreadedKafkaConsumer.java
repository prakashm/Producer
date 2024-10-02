package com.hellokafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MultiThreadedKafkaConsumer {

    public static void readMessage(String bootstrapServers, String groupId, String topic, int numConsumers) {
        // String bootstrapServers = "localhost:9092"; // Kafka broker address
        // String groupId = "multi_thread_group";
        // String topic = "your_kafka_topic"; // Kafka topic

        // int numConsumers = 3; // Number of consumer threads to spawn

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < numConsumers; i++) {
            // Create and start each consumer thread
            ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic);
            Thread consumerThread = new Thread(consumerRunnable);
            threads.add(consumerThread);
            consumerThread.start();
        }

        // Add shutdown hook to gracefully close threads
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down consumer threads...");
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }));
    }
}
