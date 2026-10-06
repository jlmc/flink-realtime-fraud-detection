package io.github.jlmc.fraud.adapter.out.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/** Serialises any value as JSON to a fixed topic. The key (may be null) decides the partition. */
public class JsonKafkaRecordSerializer<T> implements KafkaRecordSerializationSchema<T> {

    /** Function that is also {@link Serializable}, so it can travel with the operator. */
    public interface KeyFunction<T> extends Function<T, String>, Serializable {
    }

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    private final String topic;
    private final KeyFunction<T> key;

    public JsonKafkaRecordSerializer(String topic, KeyFunction<T> key) {
        this.topic = topic;
        this.key = key;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, KafkaSinkContext context, Long timestamp) {
        try {
            String k = key.apply(element);
            return new ProducerRecord<>(topic, null,
                    k == null ? null : k.getBytes(StandardCharsets.UTF_8),
                    MAPPER.writeValueAsBytes(element));
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialise " + element, e);
        }
    }
}
