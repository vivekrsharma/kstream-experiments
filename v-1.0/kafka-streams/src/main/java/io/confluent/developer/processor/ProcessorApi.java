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

public class ProcessorApi {

    static class TotalPriceOrderProcessorSupplier implements ProcessorSupplier<String, ElectronicOrder, String, Double> {
        final String storeName;

        public TotalPriceOrderProcessorSupplier(String storeName) {
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
                    // this.context.schedule(Duration.ofSeconds(30), PunctuationType.STREAM_TIME, this::forwardAll);
                    this.context.schedule(Duration.ofMillis(1), PunctuationType.STREAM_TIME, this::forwardAll);
                }

                private void forwardAll(final long timestamp) {
                   // Get a KeyValueIterator HINT there's a method on the KeyValueStore
                   // Don't forget to close the iterator! HINT use try-with resources
                   // Iterate over the records and create a Record instance and forward downstream HINT use a method on the ProcessorContext to forward
                    try (KeyValueIterator<String, Double> iterator = store.all()) {
                        while (iterator.hasNext()) {
                            final KeyValue<String, Double> nextKV = iterator.next();
                            final Record<String, Double> totalPriceRecord = new Record<>(nextKV.key, nextKV.value, timestamp);
                            context.forward(totalPriceRecord);
                        }
                    }
                }

                @Override
                public void process(Record<String, ElectronicOrder> record) {
                    // Get the current total from the store HINT: use the key on the record
                    // Don't forget to check for null
                    // Add the price from the value to the current total from store and put it in the store
                    // HINT state stores are key-value stores
                    final String key = record.key();
                    Double currentTotal = store.get(key);
                    if (currentTotal == null) {
                        currentTotal = 0.0;
                    }
                    //Double newTotal = record.value().getPrice() + currentTotal;
                    Double newTotal = 1 + currentTotal;
                    store.put(key, newTotal);
                    System.out.println(">>>>>>> Key: " + key + " Value: " + newTotal);
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

        final Topology topology = new Topology();
        topology.addSource(
            "source-node",
            stringSerde.deserializer(),
            electronicSerde.deserializer(),
            inputTopic);

        topology.addProcessor(
            "aggregate-price",
            new TotalPriceOrderProcessorSupplier(storeName),
            "source-node");

        topology.addSink(
            "sink-node",
            outputTopic,
            stringSerde.serializer(),
            doubleSerde.serializer(),
            "aggregate-price");

        // Add a source node to the topology  HINT: topology.addSource
        // Give it a name, add deserializers for the key and the value and provide the input topic name
       
        // Now add a processor to the topology HINT topology.addProcessor
        // You'll give it a name, add a processor supplier HINT: a new instance and provide the store name
        // You'll also provide a parent name HINT: it's the name you used for the source node

        // Finally, add a sink node HINT topology.addSink
        // As before give it a name, the output topic name, serializers for the key and value HINT: string and double
        // and the name of the parent node HINT it's the name you gave the processor


        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps);
        // TopicLoader.runProducer();
        kafkaStreams.start();
    }

}
