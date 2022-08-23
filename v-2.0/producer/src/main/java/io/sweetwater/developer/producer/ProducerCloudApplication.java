package io.sweetwater.developer.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Instant;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationStartedEvent;

import io.sweetwater.developer.avro.ElectronicOrder;
import org.apache.kafka.clients.producer.ProducerRecord;
import lombok.RequiredArgsConstructor;

/**
ProducerCloudApplication class uses Spring boot to create a Producer.
Uses KafkaTemplate to produce message to global source topic.
 */
@SpringBootApplication
public class ProducerCloudApplication {
	public static void main(String[] args) {
		SpringApplication.run(ProducerCloudApplication.class, args);
	}
}

@RequiredArgsConstructor
@Component
class Producer {
    private final KafkaTemplate<String, ElectronicOrder> kafkaTemplate;

	@EventListener(ApplicationStartedEvent.class)
	public void generate() {
		// Sample message
		Instant instant = Instant.now();
		ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
			.setElectronicId("101")
			.setOrderId("instore-1")
			.setUserId("10261998")
			.setPrice(2000.00)
			.setTime(instant.toEpochMilli()).build();

		ProducerRecord<String, ElectronicOrder> producerRecord = new ProducerRecord<>("processor-input-topic",
			0,
			//electronicOrder.getTime(),
			System.currentTimeMillis(),
			electronicOrderOne.getElectronicId(),
			electronicOrderOne);
		
		// Send message using KafkaTemplate
		kafkaTemplate.send(producerRecord);
   }
}