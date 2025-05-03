package com.soict.kafkabasics;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKey.class.getSimpleName());


    public static void main(String[] args) {
        log.info("hello world");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        properties.setProperty("batch.size", "400");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j =0; j< 10; j++) {
            for (int i = 0; i < 10; i++) {
                String key = "id_" + i ;
                String value = "Hello world " + i;
                sendMessage(producer, "java_demo", key, value);
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        producer.flush();
        producer.close();


    }


    public static void sendMessage(KafkaProducer<String, String> producer, String topic, String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    log.info("Received new metadata. - " +
                            "Partition: " + recordMetadata.partition() +" | "+
                            "key: " + key );
                } else {
                    log.error("Error occured: " + e.getMessage());
                }
            }
        });
    }


//    public static void main(String[] args) {
//        log.info("hello world");
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
//
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());
//
////        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
//        properties.setProperty("batch.size", "400");
//
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//
//        for (int j = 0; j < 10; j++) {
//            for (int i = 0; i < 10; i++) {
//                sendMessage(producer, "java_demo", null, "Hello World " + i);
//            }
//
//            try {
//                Thread.sleep(500);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        producer.flush();
//        producer.close();
//


    //    public static void main(String[] args) {
//        log.info("hello world");
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
//
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());
//
////        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
//        properties.setProperty("batch.size", "400");
//
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//
//        for (int j = 0; j < 10; j++) {
//            for (int i = 0; i < 10; i++) {
//                sendMessage(producer, "java_demo", null, "Hello World " + i);
//            }
//
//            try {
//                Thread.sleep(500);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        producer.flush();
//        producer.close();
//
// }
}