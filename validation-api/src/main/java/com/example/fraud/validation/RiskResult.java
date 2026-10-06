package com.example.fraud.validation;

import java.time.Instant;
import java.util.List;

/**
 * Structured outcome of risk evaluation for one transaction.
 *
 * @param riskScore 0..100
 * @param reasons   stable reason codes, e.g. {@code HIGH_TRANSACTION_VELOCITY}
 * @param timestamp event time of the evaluated transaction
 */
public record RiskResult(
        String transactionId,
        String customerId,
        int riskScore,
        RiskLevel riskLevel,
        List<String> reasons,
        Instant timestamp) {

    public RiskResult {
        if (riskScore < 0 || riskScore > 100) {
            throw new IllegalArgumentException("riskScore must be within 0..100: " + riskScore);
        }
        reasons = List.copyOf(reasons);
    }
}
