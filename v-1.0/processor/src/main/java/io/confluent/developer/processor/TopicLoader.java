/**
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

public class TopicLoader {

    public static void main(String[] args) throws IOException {
        runProducer();
    }

     public static void runProducer() throws IOException {
        Properties properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        try(Admin adminClient = Admin.create(properties);
            Producer<String, ElectronicOrder> producer = new KafkaProducer<>(properties)) {
            final String inputTopic = properties.getProperty("processor.input.topic");
            final String outputTopic = properties.getProperty("processor.output.topic");
            var topics = List.of(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
            adminClient.createTopics(topics);

            Callback callback = StreamsUtils.callback();

            Instant instant = Instant.now();
            ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
                    .setElectronicId("1001")
                    .setOrderId("instore-1")
                    .setUserId("10261998")
                    .setPrice(2000.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(60L);
            // instant = instant.plusMillis(10L);

            ElectronicOrder electronicOrderTwo = ElectronicOrder.newBuilder()
                    .setElectronicId("1002")
                    .setOrderId("instore-1")
                    .setUserId("1033737373")
                    .setPrice(1999.23)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(60L);
        // instant = instant.plusMillis(10L);

            ElectronicOrder electronicOrderThree = ElectronicOrder.newBuilder()
                    .setElectronicId("1003")
                    .setOrderId("instore-1")
                    .setUserId("1026333")
                    .setPrice(4500.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(60L);
        // instant = instant.plusMillis(10L);

            ElectronicOrder electronicOrderFour = ElectronicOrder.newBuilder()
                    .setElectronicId("1004")
                    .setOrderId("instore-1")
                    .setUserId("1038884844")
                    .setPrice(1333.98)
                    .setTime(instant.toEpochMilli()).build();


            var electronicOrders = List.of(electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour,
            electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour,
            electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour,
            electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour);

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
