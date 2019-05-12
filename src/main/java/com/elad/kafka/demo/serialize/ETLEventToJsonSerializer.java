package com.elad.kafka.demo.serialize;

import com.elad.kafka.demo.data.ETLEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by jaros on 7/5/2016.
 */
public class ETLEventToJsonSerializer implements Serializer<ETLEvent> {
    private static Logger logger = LoggerFactory.getLogger(ETLEventToJsonSerializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, ETLEvent event) {
        if (event == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(event.getContents()).getBytes();
        } catch (JsonProcessingException e) {
            logger.error(String.format("Json processing failed for object: %s", event.getClass().getName()), e);
        }
        return "".getBytes();
    }

    @Override
    public void close() {
    }
}