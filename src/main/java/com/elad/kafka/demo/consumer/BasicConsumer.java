package com.elad.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class BasicConsumer implements Runnable{

    public void basicConsumer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.249.203:8092,192.168.249.204:8092,192.168.249.204:8092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("timeout.ms", "100");
        properties.put("group.id", "test1");

        String topic = "eladTest";

        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                records.forEach(record -> {
                    System.out.println("topic=" + topic + ", value=" + record.value() + " ,key=" + record.key() + " ,partition="+ record.partition()
                            + ", offset="  + record.offset());
                });
            }

        } catch (Exception e){
            System.err.println("COnsumer failure " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
        }


    }

    @Override
    public void run() {
        basicConsumer();
    }
}
