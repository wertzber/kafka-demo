package com.elad.kafka.demo;

import com.elad.kafka.demo.consumer.BasicConsumer;
import com.elad.kafka.demo.consumer.ObjectConsumer;
import com.elad.kafka.demo.producer.BasicProducer;
import com.elad.kafka.demo.producer.HeaderProducer;
import com.elad.kafka.demo.producer.KeyProducer;
import com.elad.kafka.demo.producer.ObjectProducerSer;

public class ObjectMain {

    public static void main(String[] args) throws InterruptedException {
//object producer
//       ObjectProducerSer basicProducer = new ObjectProducerSer();
//        Thread thread1 = new Thread(basicProducer);
//        thread1.start();

//Key producer
//        KeyProducer keyProducer = new KeyProducer();
//        Thread thread1 = new Thread(keyProducer);
//        thread1.start();

        HeaderProducer headerProducer = new HeaderProducer();
        Thread thread1 = new Thread(headerProducer);
        thread1.start();

        ObjectConsumer consumer = new ObjectConsumer();
        Thread thread2 = new Thread(consumer);
        thread2.start();
    }

}
