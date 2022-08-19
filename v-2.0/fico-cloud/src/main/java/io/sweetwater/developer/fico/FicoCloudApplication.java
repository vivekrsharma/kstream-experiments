package io.sweetwater.developer.fico;

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

@SpringBootApplication
public class FicoCloudApplication {
	public static void main(String[] args) {
		SpringApplication.run(FicoCloudApplication.class, args);
	}
}

@RequiredArgsConstructor
@Component
class Producer {
    private final KafkaTemplate<String, ElectronicOrder> kafkaTemplate;

	@EventListener(ApplicationStartedEvent.class)
	public void generate() {
		// Create a sample input
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

		// Send message using kafkaTemplate	
		kafkaTemplate.send(producerRecord);
   }
}