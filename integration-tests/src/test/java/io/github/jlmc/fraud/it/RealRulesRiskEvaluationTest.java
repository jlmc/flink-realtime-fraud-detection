package io.github.jlmc.fraud.it;

import io.github.jlmc.fraud.domain.history.CustomerHistory;
import io.github.jlmc.fraud.domain.history.HistoryEntry;
import io.github.jlmc.fraud.adapter.out.plugin.ServiceLoaderRiskRuleProvider;
import io.github.jlmc.fraud.application.usecase.EvaluateRiskService;
import io.github.jlmc.fraud.domain.risk.RiskThresholds;
import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;
import io.github.jlmc.fraud.validation.Transaction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** History + the REAL fraud-rule plugins + aggregation together, still without Flink or containers. */
class RealRulesRiskEvaluationTest {

    private static final Instant T0 = Instant.parse("2026-10-06T13:00:00Z");
    private final EvaluateRiskService service = new EvaluateRiskService(new ServiceLoaderRiskRuleProvider(
            Thread.currentThread().getContextClassLoader(), Map.of(), true).rules());

    private static Transaction tx(long sec, String amount, String country) {
        return new Transaction("t-" + sec, "customer-42", "m", new BigDecimal(amount), "EUR", country, T0.plusSeconds(sec));
    }

    private RiskResult evaluate(CustomerHistory history, Transaction tx) {
        return service.evaluate(tx, history.contextFor(tx));
    }

    private static CustomerHistory feed(Transaction... txs) {
        CustomerHistory h = CustomerHistory.empty();
        for (Transaction t : txs) {
            h = h.append(new HistoryEntry(t.timestamp().toEpochMilli(), t.amount(), t.country()),
                    RiskThresholds.defaults().historyRetention(), RiskThresholds.defaults().maxHistoryEntries());
        }
        return h;
    }

    @Test
    void normalTransactionIsLowRiskWithNoReasons() {
        RiskResult r = evaluate(CustomerHistory.empty(), tx(0, "25", "PT"));

        assertThat(r.riskScore()).isZero();
        assertThat(r.riskLevel()).isEqualTo(RiskLevel.LOW);
        assertThat(r.reasons()).isEmpty();
        assertThat(r.transactionId()).isEqualTo("t-0");
        assertThat(r.timestamp()).isEqualTo(T0);
    }

    @Test
    void sixthTransactionInAMinuteTriggersVelocity() {
        CustomerHistory h = feed(tx(0, "1", "PT"), tx(5, "1", "PT"), tx(10, "1", "PT"), tx(15, "1", "PT"), tx(20, "1", "PT"));

        RiskResult r = evaluate(h, tx(25, "1", "PT"));

        assertThat(r.reasons()).containsExactly("HIGH_TRANSACTION_VELOCITY");
        assertThat(r.riskLevel()).isEqualTo(RiskLevel.MEDIUM);
    }

    @Test
    void severalRulesAddUpAndTheScoreIsCappedAt100() {
        // velocity (40) + spending (40) + geographic impossibility (50) = 130 -> 100
        CustomerHistory h = feed(tx(0, "1000", "PT"), tx(5, "1000", "US"), tx(10, "1000", "PT"), tx(15, "1000", "PT"), tx(20, "1000", "PT"));

        RiskResult r = evaluate(h, tx(25, "1000", "PT"));

        assertThat(r.reasons()).contains("HIGH_TRANSACTION_VELOCITY", "HIGH_SPENDING_VELOCITY");
        assertThat(r.riskScore()).isLessThanOrEqualTo(100);
        assertThat(r.riskLevel()).isEqualTo(RiskLevel.HIGH);
    }

    @Test
    void unusualAmountIsFlaggedAfterEnoughHistory() {
        CustomerHistory h = feed(tx(0, "20", "PT"), tx(600, "30", "PT"), tx(1200, "25", "PT"));

        RiskResult r = evaluate(h, tx(1800, "900", "PT"));

        assertThat(r.reasons()).containsExactly("UNUSUAL_AMOUNT");
    }
}
