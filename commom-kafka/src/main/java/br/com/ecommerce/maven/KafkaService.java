package br.com.ecommerce.maven;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService<T> implements Closeable{

	private final KafkaConsumer<String, T> consumer;
	private final ConsumerFunction parse;
	private String groupId;
	private Class<T> type;

	public KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
		this(parse, groupId, type, properties);
		consumer.subscribe(Collections.singletonList(topic));//escreve o consumidor para ouvir as mensagens daquele tópico
		
	}
	
	public KafkaService(String groupId, Pattern topic, ConsumerFunction parse,  Class<T> type, Map<String, String> properties) {
		this(parse, groupId, type, properties);
		consumer.subscribe(topic);//escreve o consumidor para ouvir as mensagens daquele tópico
		
	}

	public KafkaService(ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
		this.parse = parse;
		this.groupId = groupId;
		this.type = type;
		this.consumer = new KafkaConsumer<String,T>(getProperties(type, properties));//cria um consumidor kafka, com propriedades definidas
		// TODO Auto-generated constructor stub
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

	private Properties getProperties(Class<T> type, Map<String, String> overrideProperties) {
		//propriedades gerais do consumidor kafka
		//ele irá escutar a porta 9092 do endereço 127.0.0.1
		//irá deserializar uma string chave e valor
		//terá um groupId específico que é passado como parâmetro como nome da função consumidora
		//o client id será um nome randômico
		//o tamanho da poll terá como tamanho máximo 1, após 1 mensagem o kafka irá comitar.
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
		properties.putAll(overrideProperties);

		return properties;
	}

	@Override
	public void close(){
		consumer.close();		
	}

}
