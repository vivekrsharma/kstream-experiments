package io.confluent.examples.clients.cloud;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import io.confluent.examples.clients.cloud.model.DataRecord;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

/**
ConsumerExample class subscribes to sink topic.
*/
public class ConsumerExample {

  public static void main(final String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Please provide command line arguments: configPath topic");
      System.exit(1);
    }

    final String topic = args[1];
    // Load properties from a local configuration file
    // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
    final Properties props = loadConfig(args[0]);

    // Set additional properties.
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.DoubleDeserializer");
    props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, DataRecord.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-8");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Instantiate a consumer.
    final Consumer<String, Double> consumer = new KafkaConsumer<String, Double>(props);
    consumer.subscribe(Arrays.asList(topic));

    double value = 0d;
    long latency = 0l;
    try {
      while (true) {
        ConsumerRecords<String, Double> records = consumer.poll(100);
        for (ConsumerRecord<String, Double> record : records) {
          String key = record.key();
          value = record.value();
          latency = System.currentTimeMillis() - record.timestamp();
          System.out.printf("Consumed record with key %s and value %f, and latency: %d%n", key, value, latency);
        }
      }
    } finally {
      consumer.close();
    }
  }


  // loadConfig loads the kafka cluster properties from input file.
  public static Properties loadConfig(String configFile) throws IOException {
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }
    final Properties cfg = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      cfg.load(inputStream);
    }
    return cfg;
  }

}
