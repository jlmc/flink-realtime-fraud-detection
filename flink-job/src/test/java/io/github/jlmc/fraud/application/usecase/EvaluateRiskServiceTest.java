package io.github.jlmc.fraud.application.usecase;

import io.github.jlmc.fraud.domain.history.CustomerHistory;
import io.github.jlmc.fraud.validation.RiskContribution;
import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;
import io.github.jlmc.fraud.validation.Transaction;
import io.github.jlmc.fraud.validation.TransactionRiskRule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The aggregation only (sum, cap, level, order of reasons), with throwaway rules. The real rules are plugins with their own tests. */
class EvaluateRiskServiceTest {

    private static final Transaction TX = new Transaction("t-1", "customer-42", "m", new BigDecimal("10"), "EUR", "PT",
            Instant.parse("2026-10-06T13:00:00Z"));

    private static TransactionRiskRule rule(String reason, int score, boolean triggers) {
        return new TransactionRiskRule() {
            @Override
            public String name() {
                return reason;
            }

            @Override
            public RiskContribution evaluate(Transaction transaction, io.github.jlmc.fraud.validation.RiskContext context) {
                return triggers ? RiskContribution.triggered(score, reason) : RiskContribution.none();
            }
        };
    }

    private static RiskResult evaluate(TransactionRiskRule... rules) {
        return new EvaluateRiskService(List.of(rules)).evaluate(TX, CustomerHistory.empty().contextFor(TX));
    }

    @Test
    void noTriggeredRuleIsLowRiskWithNoReasons() {
        RiskResult r = evaluate(rule("A", 40, false));

        assertThat(r.riskScore()).isZero();
        assertThat(r.riskLevel()).isEqualTo(RiskLevel.LOW);
        assertThat(r.reasons()).isEmpty();
        assertThat(r.transactionId()).isEqualTo("t-1");
        assertThat(r.timestamp()).isEqualTo(TX.timestamp());
    }

    @Test
    void contributionsAddUpAndReasonsFollowTheOrderOfTheRules() {
        RiskResult r = evaluate(rule("FIRST", 40, true), rule("SKIPPED", 99, false), rule("SECOND", 30, true));

        assertThat(r.riskScore()).isEqualTo(70);
        assertThat(r.riskLevel()).isEqualTo(RiskLevel.HIGH);
        assertThat(r.reasons()).containsExactly("FIRST", "SECOND");
    }

    @Test
    void theScoreIsCappedAt100() {
        RiskResult r = evaluate(rule("A", 60, true), rule("B", 60, true));

        assertThat(r.riskScore()).isEqualTo(100);
    }

    @Test
    void theLevelFollowsTheScoreBoundaries() {
        assertThat(evaluate(rule("A", 39, true)).riskLevel()).isEqualTo(RiskLevel.LOW);
        assertThat(evaluate(rule("A", 40, true)).riskLevel()).isEqualTo(RiskLevel.MEDIUM);
        assertThat(evaluate(rule("A", 69, true)).riskLevel()).isEqualTo(RiskLevel.MEDIUM);
        assertThat(evaluate(rule("A", 70, true)).riskLevel()).isEqualTo(RiskLevel.HIGH);
    }
}
