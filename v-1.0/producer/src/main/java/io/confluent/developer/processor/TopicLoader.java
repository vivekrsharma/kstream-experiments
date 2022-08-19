package io.confluent.developer.processor;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/*
TopicLoader is a class responsible for:
1. Creating source, sink topics in Confluent cluster at the application boot.
It uses Admin APIs to do so.
2. Produce messages to source topic in serialized format.
*/
public class TopicLoader {
    public static void main(String[] args) throws IOException {
        runProducer();
    }

     public static void runProducer() throws IOException {
        // load kafka cluster properties.
        Properties properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        // Admin APIs to create topics in kafka cluster, iff they are not already created.
        try(Admin adminClient = Admin.create(properties);
            // Instantiate a producer.
            Producer<String, ElectronicOrder> producer = new KafkaProducer<>(properties)) {
            final String inputTopic = properties.getProperty("processor.input.topic");
            final String outputTopic = properties.getProperty("processor.output.topic");
            var topics = List.of(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
            adminClient.createTopics(topics);
            Callback callback = StreamsUtils.callback();

            // Define events/messages.  This can be moved to a separate class later.
            Instant instant = Instant.now();
            ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("10261998")
                    .setPrice(2000.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(60L);

            ElectronicOrder electronicOrderTwo = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1033737373")
                    .setPrice(1999.23)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(60L);

            ElectronicOrder electronicOrderThree = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1026333")
                    .setPrice(4500.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(60L);

            ElectronicOrder electronicOrderFour = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1038884844")
                    .setPrice(1333.98)
                    .setTime(instant.toEpochMilli()).build();


            // List of messages to produce.
            var electronicOrders = List.of(electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour,
            electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour,
            electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour,
            electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour);

            // Using Produer.send() to publish messages to Kafka topic.
            electronicOrders.forEach((electronicOrder -> {
                ProducerRecord<String, ElectronicOrder> producerRecord = new ProducerRecord<>(inputTopic,
                        0,
                        //electronicOrder.getTime(),
                        System.currentTimeMillis(),
                        electronicOrder.getElectronicId(),
                        electronicOrder);
                producer.send(producerRecord, callback);
            }));
        }
    }
}
