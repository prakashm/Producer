package com.hellokafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.*;

public class ExcelToKafkaWithThreads {

    public void processFile(String excelFilePath, String kafkaTopic, String kafkaBootstrapServers) {
        // Set up Kafka Producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Create a thread pool with a single thread to handle file reading
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        // Submit the file reading task as a callable
        Future<?> future = executorService.submit(() -> {
            try (FileInputStream fis = new FileInputStream(excelFilePath);
                    XSSFWorkbook workbook = new XSSFWorkbook(fis)) {

                Sheet sheet = workbook.getSheetAt(0); // Get the first sheet
                ArrayList<String> batchMessages = new ArrayList<>();

                // Create a start message with a custom header
                ProducerRecord<String, String> startRecord = new ProducerRecord<>(kafkaTopic, "key", "START");
                startRecord.headers().add(new RecordHeader("start-marker", "true".getBytes()));
                producer.send(startRecord);

                int rowCount = 0;
                for (Row row : sheet) {
                    // Convert the row to a CSV-like format
                    StringBuilder rowData = new StringBuilder();
                    row.forEach(cell -> rowData.append(cell.toString()).append(","));
                    rowData.setLength(rowData.length() - 1); // Remove last comma

                    batchMessages.add(rowData.toString());

                    // Send 100 rows as a batch
                    if (++rowCount % 100 == 0) {
                        sendBatchToKafka(producer, kafkaTopic, batchMessages);
                        batchMessages.clear();
                    }
                }

                // Send remaining rows if there are less than 100 rows left
                if (!batchMessages.isEmpty()) {
                    sendBatchToKafka(producer, kafkaTopic, batchMessages);
                }

                // Create an end message with a custom header
                ProducerRecord<String, String> endRecord = new ProducerRecord<>(kafkaTopic, "key", "END");
                endRecord.headers().add(new RecordHeader("end-marker", "true".getBytes()));
                producer.send(endRecord);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // Shutdown executor after task completion
        try {
            future.get(); // Wait for the file reading and Kafka sending to complete
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
            producer.close();
        }
    }

    // Helper method to send a batch of messages to Kafka
    private void sendBatchToKafka(Producer<String, String> producer, String topic,
            ArrayList<String> batchMessages) {
        String batchMessage = String.join("\n", batchMessages);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, batchMessage);

        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Sent batch to partition %d with offset %d%n", metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
