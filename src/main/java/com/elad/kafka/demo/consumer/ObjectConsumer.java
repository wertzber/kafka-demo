package com.elad.kafka.demo.consumer;

import com.elad.kafka.demo.data.ETLEvent;
import com.elad.kafka.demo.serialize.ETLEventDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class ObjectConsumer implements Runnable{

    public StringDeserializer desrilzer = new StringDeserializer();

    public void basicConsumer(){

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.249.203:8092,192.168.249.204:8092,192.168.249.204:8092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.elad.kafka.demo.serialize.ETLEventDeserializer");
        properties.put("timeout.ms", "1000");
        properties.put("group.id", "test3");
        properties.put("auto.offset.reset", "latest");

        String topic = "eladTest";

        KafkaConsumer<String, ETLEvent> consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while(true){
                ConsumerRecords<String, ETLEvent> records = consumer.poll(100);
                records.forEach(record -> {
                    System.out.println("###Consumer ### topic=" + topic + ", value=" + record.value() + " ,key=" + record.key()
                            + " headers[type]=" + desrilzer.deserialize(topic, record.headers().lastHeader("type").value()) +",partition="+ record.partition()
                            + ", offset="  + record.offset() );
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
