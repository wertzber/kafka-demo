package com.elad.kafka.demo.producer;

import com.elad.kafka.demo.data.ETLEvent;
import com.elad.kafka.demo.data.SodaEvent;
import net.andreinc.mockneat.MockNeat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectProducerSer implements Runnable{
    protected MockNeat mock = MockNeat.old();
    protected AtomicInteger ai = new AtomicInteger(0);


    public void objectProducer() throws InterruptedException {
        Properties properties = new Properties();
        //properties.put("bootstrap.servers", "192.168.99.101:9093");
        properties.put("bootstrap.servers", "192.168.249.203:8092,192.168.249.204:8092,192.168.249.204:8092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.elad.kafka.demo.serialize.ETLEventSerializer");
        properties.put("timeout.ms", "30000");
        String topic = "eladTest";

        Producer<String, ETLEvent> producer = new KafkaProducer<String, ETLEvent>(properties);

        while(true){
            ProducerRecord<String,ETLEvent> record= new ProducerRecord(topic, createSodaEvent());
            producer.send(record, new BasicProducerCallback());
            Thread.sleep(1000);
        }
    }


    protected String getUUID() {
        return UUID.randomUUID().toString();
    }


    @Override
    public void run() {
        try {
            objectProducer();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ETLEvent createSodaEvent(){
        SodaEvent sodaEvent = new SodaEvent();
        sodaEvent.put("name", mock.names().first().val());
        sodaEvent.put("age", mock.ints().range(0, 30).val());
        sodaEvent.put("id", getUUID());
        sodaEvent.put("ts", System.currentTimeMillis());
        System.out.println(sodaEvent);
        return sodaEvent;
    }
}
