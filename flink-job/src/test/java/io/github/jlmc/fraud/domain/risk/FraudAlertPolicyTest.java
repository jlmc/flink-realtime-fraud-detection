package io.github.jlmc.fraud.domain.risk;

import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FraudAlertPolicyTest {

    @ParameterizedTest(name = "score {0} -> alert {1}")
    @CsvSource({"0,false", "39,false", "40,false", "69,false", "70,true", "71,true", "100,true"})
    void alertsExactlyFromTheHighRiskThreshold(int score, boolean expected) {
        var result = new RiskResult("t", "c", score, RiskLevel.fromScore(score), List.of(), Instant.EPOCH);

        assertThat(FraudAlertPolicy.shouldAlert(result)).isEqualTo(expected);
    }

    @Test
    void theDecisionFollowsTheLevelOnly() {
        assertThat(FraudAlertPolicy.shouldAlert(RiskLevel.LOW)).isFalse();
        assertThat(FraudAlertPolicy.shouldAlert(RiskLevel.MEDIUM)).isFalse();
        assertThat(FraudAlertPolicy.shouldAlert(RiskLevel.HIGH)).isTrue();
    }
}
