package com.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import java.util.Arrays;
import java.util.Properties;

public class KafkaTest {
    public static void main(String[] args) {


            Properties props = new Properties();
            // 定义kakfa 服务的地址，不需要将所有broker指定上
            props.put("bootstrap.servers", "hadoop102:9092");
            // 制定consumer group
            props.put("group.id", "test");
            // 是否自动确认offset
            props.put("enable.auto.commit", "true");
            // 自动确认offset的时间间隔
            props.put("auto.commit.interval.ms", "1000");
            // key的序列化类
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            // value的序列化类
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            // 定义consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

            // 指定要消费的topic, 可同时处理多个
            consumer.subscribe(Arrays.asList("first", "second","third"));

            while (true) {
                // 读取数据，读取超时时间为100ms
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }


    }
}
