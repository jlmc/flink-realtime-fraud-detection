package io.github.jlmc.fraud.validation;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Transaction event as received from Kafka.
 *
 * <p>Fields are intentionally nullable: structural validation (required fields) is the
 * responsibility of validation rules, not of this type.
 *
 * <p>{@code paymentStatus} is optional in the payload: a missing value means the attempt was approved, so producers
 * written before the field existed keep working. An unknown value is a malformed payload.
 */
public record Transaction(
        String transactionId,
        String customerId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String country,
        Instant timestamp,
        PaymentStatus paymentStatus) {

    /** A transaction without a payment status, i.e. an approved one. */
    public Transaction(String transactionId, String customerId, String merchantId, BigDecimal amount,
                       String currency, String country, Instant timestamp) {
        this(transactionId, customerId, merchantId, amount, currency, country, timestamp, null);
    }

    /** True only for an explicitly declined attempt. */
    public boolean isDeclined() {
        return paymentStatus == PaymentStatus.DECLINED;
    }
}
