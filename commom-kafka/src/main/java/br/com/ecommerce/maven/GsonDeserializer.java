package br.com.ecommerce.maven;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonDeserializer<T> implements Deserializer<T>{
	public static final String TYPE_CONFIG = "br.com.ecommerce.maven";
	private final Gson gson = new GsonBuilder().create();
	private Class<T> type;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		String typeName = String.valueOf(configs.get(TYPE_CONFIG));
		try {
			this.type = (Class<T>) Class.forName(typeName);
		} catch (Exception e) {
			throw new RuntimeException("Type for deserialization does not exist in the classpath");
		}
	}
	
	@Override
	public T deserialize(String topic, byte[] bytes) {
		return gson.fromJson(new String(bytes), type);
	}
	
}
