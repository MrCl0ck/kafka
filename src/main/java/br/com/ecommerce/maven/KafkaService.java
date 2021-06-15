package br.com.ecommerce.maven;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService<T> implements Closeable{

	private final KafkaConsumer<String, String> consumer;
	private final ConsumerFunction parse;
	private String groupId;

	public KafkaService(String groupId, String topic, ConsumerFunction parse) {
		this.groupId = groupId;
		this.parse = parse;
		this.consumer = new KafkaConsumer<String,String>(properties());//cria um consumidor kafka, com propriedades definidas
		consumer.subscribe(Collections.singletonList(topic));//escreve o consumidor para ouvir as mensagens daquele tópico
		
	}
	
	public KafkaService(String groupId, Pattern topic, ConsumerFunction parse) {
		this.groupId = groupId;
		this.parse = parse;
		this.consumer = new KafkaConsumer<String,String>(properties());//cria um consumidor kafka, com propriedades definidas
		consumer.subscribe(topic);//escreve o consumidor para ouvir as mensagens daquele tópico
		
	}

	void run() {
		while(true) {
			var records = consumer.poll(Duration.ofMillis(100));//usa 100 milisegundos para ouvir as mensagens
			if (!records.isEmpty()) {
				//caso exista mensagens entra na condição
				System.out.println("Encontrei " + records.count() + " registros");
				for(var record: records) {
					//executa a função parse, interface...
					parse.consume(record);					
				}
			}
		}
	}

	private Properties properties() {
		//propriedades gerais do consumidor kafka
		//ele irá escutar a porta 9092 do endereço 127.0.0.1
		//irá deserializar uma string chave e valor
		//terá um groupId específico que é passado como parâmetro como nome da função consumidora
		//o client id será um nome randômico
		//o tamanho da poll terá como tamanho máximo 1, após 1 mensagem o kafka irá comitar.
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

		return properties;
	}

	@Override
	public void close(){
		consumer.close();		
	}

}
