package br.com.ecommerce.maven;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {
	public static void main(String[] args) {
		var fraudService = new FraudDetectorService();
		try(var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudService::parse)){
			service.run();
		}
	}
	
	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("-----------------------------------------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println("Chave: " + record.key());
		System.out.println("Valor: " + record.value());
		System.out.println("Partição: " + record.partition());
		System.out.println("Offset: " + record.offset());	
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Order processed");
	}
}
