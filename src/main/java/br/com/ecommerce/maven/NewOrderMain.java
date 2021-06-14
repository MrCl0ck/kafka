package br.com.ecommerce.maven;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var producer = new KafkaProducer<String, String>(properties());
		var value = UUID.randomUUID().toString();
		var key = "558749-6327";
		var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, key);
		
		
		
		Callback callback = (data,ex) -> {
			if(ex != null) {
				ex.printStackTrace();
				return;
			}
			System.out.println("Sucesso enviando... "  + data.topic() + ":::partition " + data.partition() + " /offset " + data.offset() + " /timestamp " + data.timestamp());
		};
		var email = "Thank you for your order! We are processing your order"; 
		var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
		producer.send(record, callback).get();		
		producer.send(emailRecord, callback).get();
		
	}

	private static Properties properties() {
		var properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		return properties;		
	}
}

