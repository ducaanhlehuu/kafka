package com.soict.kafka.wikimedia;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {

	public static void main(String[] args) throws InterruptedException {

		String bootstrapServer = "127.0.0.1:9092";
		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Some setting for kafka version <= 2.8
//		properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
//		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
//		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // = 1 nếu version kafka version < 1.0
		// Để cho đỡ mất data, mất order

		// Setting để gom batch và nén dữ liệu
		// Setting for batching message and compress message in producer
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "15");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


		String topic = "wikimedia_recent_change";
		String url = "https://stream.wikimedia.org/v2/stream/recentchange";
		EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

		EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(url)).build();

		try {
			eventSource.start();
			TimeUnit.MINUTES.sleep(10);  // chạy trong 10 phút
		} finally {
			eventSource.close();
			producer.close();
		}
	}

}
