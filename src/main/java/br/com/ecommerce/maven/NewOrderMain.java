package br.com.ecommerce.maven;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		try(var dispatcher = new KafkaDispatcher()){
			for (int i = 0; i < 100; i++) {
				var key = UUID.randomUUID().toString();
				var value = key + " - 558749-6327";

				var email = "Thank you for your order! We are processing your order!";
				dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
				dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
			}
		}
	}
}

