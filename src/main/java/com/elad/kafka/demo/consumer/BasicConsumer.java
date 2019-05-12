package com.elad.kafka.demo.consumer;

import java.util.Properties;

public class BasicConsumer {

    public void basicConsumer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.249.203:8092,192.168.249.204:8092,192.168.249.204:8092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("timeout.ms", "100");
        properties.put("group.id", "test1");


        String topic = "eladTest";
    }
}
