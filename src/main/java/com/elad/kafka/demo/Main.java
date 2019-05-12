package com.elad.kafka.demo;

import com.elad.kafka.demo.consumer.BasicConsumer;
import com.elad.kafka.demo.producer.BasicProducer;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        BasicProducer basicProducer = new BasicProducer();
        Thread thread1 = new Thread(basicProducer);
        thread1.start();

        BasicConsumer consumer = new BasicConsumer();
        Thread thread2 = new Thread(consumer);
        thread2.start();
    }

}
