package com.elad.kafka.demo.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class BasicProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception!=null){
            System.err.println("Failed to send msg \n" + exception.getMessage() + "\n"  );
            exception.printStackTrace();
        } else{
            System.out.println("### Send msg success!!!!");
        }
    }
}
