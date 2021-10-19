package com.example.bean.streaming.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CloudEventsConsumerApplication implements CommandLineRunner {

	public static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";

	public static final String KAFKA_TOPIC = "my-cloud-events";

	public static void main(String[] args) {
		SpringApplication.run(CloudEventsConsumerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// Basic consumer configuration
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-cloudevents-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

		// Create the consumer and subscribe to the topic
		KafkaConsumer<String, CloudEvent> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));

		System.out.println("Consumer started");

		while (true) {
			ConsumerRecords<String, CloudEvent> consumerRecords = consumer.poll(Duration.ofMillis(100));
			consumerRecords.forEach(record -> {
				System.out.println(
						"New record:\n" +
								"  Record Key " + record.key() + "\n" +
								"  Record value " + record.value() + "\n" +
								"  Record partition " + record.partition() + "\n" +
								"  Record offset " + record.offset() + "\n"
				);
			});
		}
	}
}
