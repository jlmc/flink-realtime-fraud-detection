package io.github.jlmc.fraud.adapter.out.kafka;

import io.github.jlmc.fraud.application.model.HighRiskFraudAlert;
import io.github.jlmc.fraud.application.model.InvalidEvent;
import io.github.jlmc.fraud.validation.RiskResult;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public final class KafkaSinks {

    private KafkaSinks() {
    }

    /**
     * Risk results: EXACTLY_ONCE (Kafka transactions, committed on checkpoint completion). Consumers must read with
     * {@code isolation.level=read_committed}. {@code transaction.timeout.ms} must cover the longest checkpoint plus
     * the longest restart, otherwise Kafka aborts pending transactions and data is lost; it must also stay at or
     * below the broker's {@code transaction.max.timeout.ms} (default 15 min).
     */
    public static KafkaSink<RiskResult> riskResults(String bootstrapServers, String topic,
                                                    String transactionalIdPrefix, int transactionTimeoutMs) {
        return KafkaSink.<RiskResult>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new JsonKafkaRecordSerializer<RiskResult>(topic, RiskResult::customerId))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionalIdPrefix)
                .setProperty("transaction.timeout.ms", String.valueOf(transactionTimeoutMs))
                .build();
    }

    /** Invalid events: AT_LEAST_ONCE. A duplicated dead-letter record is harmless; a transaction per record is not free. */
    public static KafkaSink<InvalidEvent> invalidEvents(String bootstrapServers, String topic) {
        return KafkaSink.<InvalidEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new JsonKafkaRecordSerializer<InvalidEvent>(topic, InvalidEvent::transactionId))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /**
     * High-risk alerts: same strategy as the risk results (EXACTLY_ONCE, read_committed consumers). The prefix must be
     * different from the other transactional sink's. Keyed by customer so alerts of one customer stay ordered.
     */
    public static KafkaSink<HighRiskFraudAlert> highRiskAlerts(String bootstrapServers, String topic,
                                                               String transactionalIdPrefix, int transactionTimeoutMs) {
        return KafkaSink.<HighRiskFraudAlert>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new JsonKafkaRecordSerializer<HighRiskFraudAlert>(topic, HighRiskFraudAlert::customerId))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionalIdPrefix)
                .setProperty("transaction.timeout.ms", String.valueOf(transactionTimeoutMs))
                .build();
    }
}
