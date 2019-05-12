package com.elad.kafka.demo;

import com.elad.kafka.demo.producer.BasicProducer;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        BasicProducer basicProducer = new BasicProducer();
        basicProducer.basicProducer();
    }

}
