package com.oujian.gmall.dw.canal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class MyKafkaSend {
    private static KafkaProducer<String,String> createProduce(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop100:9092,hadoop101:9092,hadoop102:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }
    //发送kafka消息
    public static void send(String topic,String message){
        KafkaProducer<String, String> produce = createProduce();
        produce.send(new ProducerRecord<>(topic, message));
    }
}
