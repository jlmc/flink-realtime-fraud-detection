package io.github.jlmc.fraud.application.model;

import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HighRiskFraudAlertTest {

    private static final RiskResult RESULT = new RiskResult("tx-123", "customer-42", 97, RiskLevel.HIGH,
            List.of("HIGH_TRANSACTION_VELOCITY", "UNUSUAL_AMOUNT"), Instant.parse("2026-10-06T13:10:01Z"));

    @Test
    void carriesTheWholeRiskResult() {
        var alert = HighRiskFraudAlert.from(RESULT);

        assertThat(alert.transactionId()).isEqualTo("tx-123");
        assertThat(alert.customerId()).isEqualTo("customer-42");
        assertThat(alert.riskScore()).isEqualTo(97);
        assertThat(alert.riskLevel()).isEqualTo(RiskLevel.HIGH);
        assertThat(alert.reasons()).containsExactly("HIGH_TRANSACTION_VELOCITY", "UNUSUAL_AMOUNT");
        assertThat(alert.transactionTimestamp()).isEqualTo(Instant.parse("2026-10-06T13:10:01Z"));
    }

    @Test
    void theIdIsDeterministicSoReprocessingProducesTheSameAlert() {
        assertThat(HighRiskFraudAlert.from(RESULT).alertId()).isEqualTo("high-risk-v1-tx-123");
        assertThat(HighRiskFraudAlert.from(RESULT)).isEqualTo(HighRiskFraudAlert.from(RESULT));
    }
}
