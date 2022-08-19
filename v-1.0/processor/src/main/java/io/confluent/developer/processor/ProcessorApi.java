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

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static io.confluent.developer.StreamsUtils.*;

/*
ProcessorApi class performs:
1. Creates a topology using processor APIs.
2. Perform an aggregation operation using a state store.
3. Publish messages to sink topic.
*/
public class ProcessorApi {

    static class ProcessorStore implements ProcessorSupplier<String, ElectronicOrder, String, Double> {
        final String storeName;

        public ProcessorStore(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public Processor<String, ElectronicOrder, String, Double> get() {
            return new Processor<>() {
                private ProcessorContext<String, Double> context;
                private KeyValueStore<String, Double> store;

                @Override
                public void init(ProcessorContext<String, Double> context) {
                    // Save reference of the context
                    // Retrieve the store and save a reference
                    // Schedule a punctuation  HINT: use context.schedule and the method you want to call is forwardAll
                    this.context = context;
                    store = context.getStateStore(storeName);
                    this.context.schedule(Duration.ofMillis(1), PunctuationType.STREAM_TIME, this::forwardAll);
                }

                private void forwardAll(final long timestamp) {
                   // Get a KeyValueIterator HINT there's a method on the KeyValueStore
                   // Don't forget to close the iterator! HINT use try-with resources
                   // Iterate over the records and create a Record instance and forward downstream HINT use a method on the ProcessorContext to forward
                    try (KeyValueIterator<String, Double> iterator = store.all()) {
                        while (iterator.hasNext()) {
                            final KeyValue<String, Double> nextKV = iterator.next();
                            final Record<String, Double> r = new Record<>(nextKV.key, nextKV.value, timestamp);
                            context.forward(r);
                        }
                    }
                }

                // This is the first aggreagation operation.
                // This can be separated out into another package later to scale up aggragations.
                // As of now, it performs a simple aggregation to maintain total count of the input key.
                @Override
                public void process(Record<String, ElectronicOrder> record) {
                    final String key = record.key();
                    Double currentTotal = store.get(key);
                    if (currentTotal == null) {
                        currentTotal = 0.0;
                    }
                    Double newTotal = 1 + currentTotal;
                    store.put(key, newTotal);
                }
            };
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Collections.singleton(totalPriceStoreBuilder);
        }
    }

    final static String storeName = "total-price-store-new9";
    static StoreBuilder<KeyValueStore<String, Double>> totalPriceStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName),
            Serdes.String(),
            Serdes.Double());

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-application");

        final String inputTopic = streamsProps.getProperty("processor.input.topic");
        final String outputTopic = streamsProps.getProperty("processor.output.topic");
        final Map<String, Object> configMap = propertiesToMap(streamsProps);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();

        // Define processor topology.  The flow would be:
        // source topic -> intermediate topic -> sink topic
        final Topology topology = new Topology();
        topology.addSource(
            "source-node",
            stringSerde.deserializer(),
            electronicSerde.deserializer(),
            inputTopic);

        topology.addProcessor(
            "aggregate-price",
            new ProcessorStore(storeName),
            "source-node");

        topology.addSink(
            "sink-node",
            outputTopic,
            stringSerde.serializer(),
            doubleSerde.serializer(),
            "aggregate-price");

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps);
        kafkaStreams.start();
    }

}
