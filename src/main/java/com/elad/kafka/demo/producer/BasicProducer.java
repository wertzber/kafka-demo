package com.elad.kafka.demo.producer;

import net.andreinc.mockneat.MockNeat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class BasicProducer {
    protected MockNeat mock = MockNeat.old();
    protected AtomicInteger ai = new AtomicInteger(0);


    public void basicProducer() throws InterruptedException {
        Properties properties = new Properties();
        //properties.put("bootstrap.servers", "192.168.99.101:9093");
        properties.put("bootstrap.servers", "192.168.249.203:8092,192.168.249.204:8092,192.168.249.204:8092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("timeout.ms", "30000");
        String topic = "eladTest";

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        while(true){
            ProducerRecord<String,String> record= new ProducerRecord(topic, getUUID(), "elad" + ai.incrementAndGet());
            producer.send(record, new BasicProducerCallback());
        }
    }


    protected String getUUID() {
        return UUID.randomUUID().toString();
    }


}
