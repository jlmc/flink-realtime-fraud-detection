package io.github.jlmc.fraud.application.model;

import io.github.jlmc.fraud.validation.Transaction;

/**
 * A message as it arrived from the transport, together with the outcome of parsing it. Parsing never throws:
 * a message that cannot be parsed carries {@code error} and no transaction, so poison messages travel through the
 * pipeline as data and are routed to the invalid-events topic instead of crashing the job.
 *
 * @param transaction parsed transaction, or {@code null} when parsing failed
 * @param error       parse failure description, or {@code null} when parsing succeeded
 * @param payload     original payload decoded as UTF-8 (lossy for non-text payloads), for diagnostics
 */
public record IncomingMessage(
        Transaction transaction,
        String error,
        String payload,
        String topic,
        int partition,
        long offset) {

    public boolean isParsed() {
        return transaction != null;
    }
}
