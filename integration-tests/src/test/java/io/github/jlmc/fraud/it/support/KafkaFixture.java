package io.github.jlmc.fraud.it.support;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** One Kafka broker (KRaft, same image version as the compose stack) per JVM; every test gets its own topics. */
public final class KafkaFixture {

    private static final KafkaContainer CONTAINER = new KafkaContainer("apache/kafka:4.3.1");
    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final ObjectMapper JSON = new ObjectMapper();

    static {
        CONTAINER.start();
    }

    private KafkaFixture() {
    }

    public record Topics(String events, String risk, String invalid, String alerts, String consumerGroup) {
    }

    public static String bootstrapServers() {
        return CONTAINER.getBootstrapServers();
    }

    /** Fresh, uniquely named topics. One partition keeps the order of the output easy to assert. */
    public static Topics newTopics() {
        String suffix = COUNTER.incrementAndGet() + "-" + UUID.randomUUID().toString().substring(0, 8);
        Topics topics = new Topics("events-" + suffix, "risk-" + suffix, "invalid-" + suffix, "alerts-" + suffix, "group-" + suffix);
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topics.events(), 1, (short) 1), new NewTopic(topics.risk(), 1, (short) 1),
                    new NewTopic(topics.invalid(), 1, (short) 1), new NewTopic(topics.alerts(), 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("cannot create topics", e);
        }
        return topics;
    }

    /** Sends raw payloads, in order, and waits until the broker has them. */
    public static void send(String topic, String... payloads) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String payload : payloads) {
                producer.send(new ProducerRecord<>(topic, payload));
            }
            producer.flush();
        }
    }

    /** Everything currently in the topic, from the beginning. Read-committed, so uncommitted transactional output is invisible. */
    public static List<String> readAll(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            int quietPolls = 0;
            while (quietPolls < 2) {
                var records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    quietPolls++;
                } else {
                    quietPolls = 0;
                    records.forEach(r -> values.add(r.value()));
                }
            }
        }
        return values;
    }

    /** Same as {@link #readAll} with every JSON object parsed into a map. Non-JSON values are skipped. */
    public static List<Map<String, Object>> readAllJson(String topic) {
        List<Map<String, Object>> parsed = new ArrayList<>();
        for (String value : readAll(topic)) {
            try {
                parsed.add(JSON.readValue(value, new TypeReference<>() {
                }));
            } catch (Exception ignored) {
                // not JSON
            }
        }
        return parsed;
    }
}
