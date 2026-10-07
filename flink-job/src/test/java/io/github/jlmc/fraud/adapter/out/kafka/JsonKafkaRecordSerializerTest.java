package io.github.jlmc.fraud.adapter.out.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jlmc.fraud.application.model.HighRiskFraudAlert;
import io.github.jlmc.fraud.application.model.InvalidEvent;
import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JsonKafkaRecordSerializerTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void riskResultMatchesTheDocumentedShape() throws Exception {
        var serializer = new JsonKafkaRecordSerializer<RiskResult>("transaction.risk.events", RiskResult::customerId);
        var result = new RiskResult("tx-123", "customer-42", 85, RiskLevel.HIGH,
                List.of("HIGH_TRANSACTION_VELOCITY", "UNUSUAL_AMOUNT"), Instant.parse("2026-10-06T13:10:01Z"));

        ProducerRecord<byte[], byte[]> record = serializer.serialize(result, null, null);
        JsonNode json = JSON.readTree(record.value());

        assertThat(record.topic()).isEqualTo("transaction.risk.events");
        assertThat(new String(record.key(), StandardCharsets.UTF_8)).isEqualTo("customer-42");
        assertThat(json.get("transactionId").asText()).isEqualTo("tx-123");
        assertThat(json.get("customerId").asText()).isEqualTo("customer-42");
        assertThat(json.get("riskScore").asInt()).isEqualTo(85);
        assertThat(json.get("riskLevel").asText()).isEqualTo("HIGH");
        assertThat(json.get("reasons")).hasSize(2);
        assertThat(json.get("timestamp").asText()).isEqualTo("2026-10-06T13:10:01Z");
    }

    @Test
    void invalidEventWithoutTransactionIdHasNoKey() throws Exception {
        var serializer = new JsonKafkaRecordSerializer<InvalidEvent>("transaction.invalid.events", InvalidEvent::transactionId);
        var event = new InvalidEvent("DESERIALIZATION", "MALFORMED_PAYLOAD", "bad", null, "garbage", "transaction.events", 1, 7L,
                Instant.parse("2026-10-06T13:10:01Z"));

        ProducerRecord<byte[], byte[]> record = serializer.serialize(event, null, null);

        assertThat(record.key()).isNull();
        assertThat(JSON.readTree(record.value()).get("payload").asText()).isEqualTo("garbage");
        assertThat(JSON.readTree(record.value()).get("source").asText()).isEqualTo("DESERIALIZATION");
    }

    @Test
    void alertIsReadableByADownstreamConsumerWithNoKnowledgeOfOurClasses() throws Exception {
        var serializer = new JsonKafkaRecordSerializer<HighRiskFraudAlert>("fraud.high-risk.alerts", HighRiskFraudAlert::customerId);
        var alert = HighRiskFraudAlert.from(new RiskResult("tx-123", "customer-42", 97, RiskLevel.HIGH,
                List.of("HIGH_TRANSACTION_VELOCITY", "UNUSUAL_AMOUNT"), Instant.parse("2026-10-06T13:10:01Z")));

        ProducerRecord<byte[], byte[]> record = serializer.serialize(alert, null, null);
        JsonNode json = JSON.readTree(record.value());   // plain Jackson tree: this is all a consumer needs

        assertThat(record.topic()).isEqualTo("fraud.high-risk.alerts");
        assertThat(new String(record.key(), StandardCharsets.UTF_8)).isEqualTo("customer-42");
        assertThat(json.get("alertId").asText()).isEqualTo("high-risk-v1-tx-123");
        assertThat(json.get("transactionId").asText()).isEqualTo("tx-123");
        assertThat(json.get("customerId").asText()).isEqualTo("customer-42");
        assertThat(json.get("riskScore").asInt()).isEqualTo(97);
        assertThat(json.get("riskLevel").asText()).isEqualTo("HIGH");
        assertThat(json.get("reasons").get(0).asText()).isEqualTo("HIGH_TRANSACTION_VELOCITY");
        assertThat(json.get("reasons").get(1).asText()).isEqualTo("UNUSUAL_AMOUNT");
        assertThat(json.get("transactionTimestamp").asText()).isEqualTo("2026-10-06T13:10:01Z");
    }
}
