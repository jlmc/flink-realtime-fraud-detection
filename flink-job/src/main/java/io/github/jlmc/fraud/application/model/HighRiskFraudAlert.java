package io.github.jlmc.fraud.application.model;

import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;

import java.time.Instant;
import java.util.List;

/**
 * An actionable fraud alert: "this transaction requires downstream fraud handling". Different from a
 * {@link RiskResult}, which only says "this transaction was evaluated".
 *
 * <p>The alert is a pure function of the risk result, so processing the same transaction again produces a byte-identical
 * record. That is why {@code alertId} is derived from the transaction id and the alert type/version instead of being random,
 * and why there is no processing-time field: consumers can deduplicate on {@code alertId}.
 *
 * @param alertId              {@code high-risk-v1-<transactionId>}; the version changes if the alert's meaning changes
 * @param riskLevel            always {@link RiskLevel#HIGH} today; kept so the record stays self-describing if the policy widens
 * @param reasons              every reason code of the risk result
 * @param transactionTimestamp event time of the transaction
 */
public record HighRiskFraudAlert(
        String alertId,
        String transactionId,
        String customerId,
        int riskScore,
        RiskLevel riskLevel,
        List<String> reasons,
        Instant transactionTimestamp) {

    private static final String ID_PREFIX = "high-risk-v1-";

    public HighRiskFraudAlert {
        reasons = List.copyOf(reasons);
    }

    public static HighRiskFraudAlert from(RiskResult result) {
        return new HighRiskFraudAlert(ID_PREFIX + result.transactionId(), result.transactionId(), result.customerId(),
                result.riskScore(), result.riskLevel(), result.reasons(), result.timestamp());
    }
}
