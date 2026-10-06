package io.github.jlmc.fraud.application.model;

import java.time.Instant;

/**
 * Something that could not be processed, published to {@code transaction.invalid.events}.
 *
 * @param source        {@code DESERIALIZATION} or {@code VALIDATION}
 * @param code          machine-readable code(s)
 * @param detail        human-readable explanation
 * @param transactionId when known
 * @param payload       original payload
 * @param detectedAt    processing time at which the problem was detected
 */
public record InvalidEvent(
        String source,
        String code,
        String detail,
        String transactionId,
        String payload,
        String topic,
        int partition,
        long offset,
        Instant detectedAt) {

    public static final String SOURCE_DESERIALIZATION = "DESERIALIZATION";
    public static final String SOURCE_VALIDATION = "VALIDATION";
}
