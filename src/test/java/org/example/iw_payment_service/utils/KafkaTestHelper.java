package org.example.iw_payment_service.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

public class KafkaTestHelper<T> {

    private final KafkaProducer<String, Object> producer;
    private final KafkaConsumer<String, T> consumer;
    public KafkaTestHelper(String bootstrapServers, String topic, Class<T> messageClass, String trustedPackages) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, trustedPackages);
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, messageClass.getName());

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));
    }
    public KafkaTestHelper(String bootstrapServers, String topic, Class<T> messageClass) {
        this(bootstrapServers, topic, messageClass, "org.example.iw_payment_service.dto");
    }

    public void sendMessage(String topic, String key, Object value) {
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, value));
            future.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to send message to Kafka", e);
        }
    }

    public List<T> consumeMessages(Duration timeout) {
        List<T> messages = new ArrayList<>();
        long endTime = System.currentTimeMillis() + timeout.toMillis();

        while (System.currentTimeMillis() < endTime) {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, T> record : records) {
                messages.add(record.value());
            }
        }
        return messages;
    }

    public void close() {
        producer.close();
        consumer.close();
    }
}
