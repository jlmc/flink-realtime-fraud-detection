package io.github.jlmc.fraud.adapter.in.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.jlmc.fraud.application.model.IncomingMessage;
import io.github.jlmc.fraud.validation.Transaction;

import java.nio.charset.StandardCharsets;

/**
 * Turns a Kafka payload into an {@link IncomingMessage}. Never throws: malformed JSON, wrong types, a non-object
 * document or an empty payload all yield a message carrying an {@code error}.
 *
 * <p>A well-formed document with missing fields is NOT a parse error: the transaction is produced with nulls and the
 * validation plugins decide whether it is acceptable.
 */
public final class TransactionJsonParser {

    public static final String ERROR_PREFIX = "MALFORMED_PAYLOAD: ";

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .build();

    private TransactionJsonParser() {
    }

    public static IncomingMessage parse(byte[] value, String topic, int partition, long offset) {
        if (value == null || value.length == 0) {
            return failure("empty payload", "", topic, partition, offset);
        }
        String payload = new String(value, StandardCharsets.UTF_8);
        try {
            JsonNode node = MAPPER.readTree(value);
            if (node == null || !node.isObject()) {
                return failure("payload is not a JSON object", payload, topic, partition, offset);
            }
            Transaction transaction = MAPPER.treeToValue(node, Transaction.class);
            return new IncomingMessage(transaction, null, payload, topic, partition, offset);
        } catch (Exception e) {
            return failure(rootMessage(e), payload, topic, partition, offset);
        }
    }

    private static IncomingMessage failure(String reason, String payload, String topic, int partition, long offset) {
        return new IncomingMessage(null, ERROR_PREFIX + reason, payload, topic, partition, offset);
    }

    private static String rootMessage(Throwable t) {
        String message = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
        int newline = message.indexOf('\n');
        return newline > 0 ? message.substring(0, newline) : message;
    }
}
