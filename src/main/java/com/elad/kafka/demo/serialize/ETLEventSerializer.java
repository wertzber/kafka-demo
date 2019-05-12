package com.elad.kafka.demo.serialize;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.elad.kafka.demo.data.ETLEvent;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serializes the ETLEvent using Protostuff.<br>
 * We wrap the event with {@link EventWrapper} in order for it to keep it's original class (otherwise when deserializing
 * we will get ETLEvent instead of its actual child).
 *
 * @author miki.b
 */
public class ETLEventSerializer implements Serializer<ETLEvent> {

	private Schema<EventWrapper> schema = RuntimeSchema.getSchema(EventWrapper.class);

	@Override
	public void configure(Map<String, ?> config, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, ETLEvent event) {
		if (event == null) {
			return null;
		}

		try {
			int size = 512;
			byte[] data = ProtostuffIOUtil.toByteArray(new EventWrapper(event), schema, LinkedBuffer.allocate(size));
			return data;
		} catch (Exception e) {
			throw new SerializationException("Failed serializing event", e);
		}
	}

	@Override
	public void close() {
	}

}
