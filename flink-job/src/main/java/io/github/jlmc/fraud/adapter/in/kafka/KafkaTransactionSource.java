package io.github.jlmc.fraud.adapter.in.kafka;

import io.github.jlmc.fraud.application.model.IncomingMessage;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class KafkaTransactionSource {

    private KafkaTransactionSource() {
    }

    /**
     * Resumes from the offsets committed by a previous run and starts from the earliest offset when there are none.
     * When the job is restored from a checkpoint or savepoint, Flink's own stored offsets take precedence; Kafka's
     * committed offsets are only used for monitoring consumer lag.
     */
    public static KafkaSource<IncomingMessage> create(String bootstrapServers, String topic, String consumerGroup) {
        return KafkaSource.<IncomingMessage>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new IncomingMessageDeserializationSchema())
                .setProperty("partition.discovery.interval.ms", "30000")
                .build();
    }
}
