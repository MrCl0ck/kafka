package br.com.ecommerce.maven;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
	public static void main(String[] args) {
		var emailService = new EmailService();
		try(var service = new KafkaService<>(EmailService.class.getSimpleName(),"ECOMMERCE_SEND_EMAIL", emailService::parse)){
			service.run();			
		}
	}

	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("-----------------------------------------------");
		System.out.println("Send email");
		System.out.println("Chave: " + record.key());
		System.out.println("Valor: " + record.value());
		System.out.println("Partição: " + record.partition());
		System.out.println("Offset: " + record.offset());	
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Email sent");
	}
}
