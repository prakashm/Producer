package com.hellokafka;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    public static void main(String[] args) {
        ExcelToKafkaWithThreads excelToKafkaWithThreads = new ExcelToKafkaWithThreads();
        excelToKafkaWithThreads.processFile("/tmp/test.xlsx", "EXCEL_FILE_101", "localhost:9092");

        System.out.println("-----------------------------------------");

        // Consume the message
        MultiThreadedKafkaConsumer consumer = new MultiThreadedKafkaConsumer();
        consumer.readMessage("localhost:9092", "multi_thread_group", "EXCEL_FILE_101", 10);

    }
}
