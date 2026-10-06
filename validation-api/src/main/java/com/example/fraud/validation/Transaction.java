package com.example.fraud.validation;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Transaction event as received from Kafka.
 *
 * <p>Fields are intentionally nullable: structural validation (required fields) is the
 * responsibility of validation rules, not of this type.
 */
public record Transaction(
        String transactionId,
        String customerId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String country,
        Instant timestamp) {
}
