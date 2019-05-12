package com.elad.kafka.demo.serialize;

import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.elad.kafka.demo.data.ETLEvent;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Deserializes the ETLEvent using Protostuff.<br>
 * The event is wrapped with {@link EventWrapper} in order for it to keep it's original class (otherwise when
 * deserializing we will get ETLEvent instead of its actual child).
 *
 * @author miki.b
 */
public class ETLEventDeserializer implements Deserializer<ETLEvent> {

	private Schema<EventWrapper> schema = RuntimeSchema.getSchema(EventWrapper.class);

	@Override
	public void configure(Map<String, ?> config, boolean isKey) {
	}

	@Override
	public ETLEvent deserialize(String topic, byte[] data) {
		Object event;
		try {
			EventWrapper eventWrapper = new EventWrapper();
			ProtostuffIOUtil.mergeFrom(data, eventWrapper, schema);
			event = eventWrapper.getEvent();
		} catch (Exception e) {
			throw new RuntimeException("Failed deserializing Kafka data", e);
		}

		if (event != null && event instanceof ETLEvent) {
			return (ETLEvent) event;
		} else {
			throw new RuntimeException("The deserialized Kafka data is not an instance of ETLEvent");
		}
	}

	@Override
	public void close() {
	}

}
