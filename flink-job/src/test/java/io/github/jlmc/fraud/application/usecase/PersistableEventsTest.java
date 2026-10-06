package io.github.jlmc.fraud.application.usecase;

import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.model.RiskOutcome;
import io.github.jlmc.fraud.application.model.TransactionStatus;
import io.github.jlmc.fraud.domain.risk.FraudAlertPolicy;
import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;
import io.github.jlmc.fraud.validation.Transaction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.jlmc.fraud.testsupport.Transactions.tx;
import static org.assertj.core.api.Assertions.assertThat;

class PersistableEventsTest {

    private static RiskOutcome outcome(int score) {
        Transaction t = tx("t1", "c", 0, "10", "PT");
        return new RiskOutcome(t, new RiskResult("t1", "c", score, RiskLevel.fromScore(score), List.of(), t.timestamp()));
    }

    @Test
    void onlyHighRiskRaisesAnAlert() {
        assertThat(PersistableEvents.processed(outcome(10)).raiseAlert()).isFalse();
        assertThat(PersistableEvents.processed(outcome(50)).raiseAlert()).isFalse();
        assertThat(PersistableEvents.processed(outcome(70)).raiseAlert()).isTrue();
        assertThat(FraudAlertPolicy.shouldAlert(RiskLevel.MEDIUM)).isFalse();
    }

    @Test
    void processedKeepsTheRiskResult() {
        PersistableEvent e = PersistableEvents.processed(outcome(85));

        assertThat(e.status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(e.risk().riskScore()).isEqualTo(85);
    }

    @Test
    void lateTransactionsAreStoredWithoutRiskAndWithoutAlert() {
        PersistableEvent e = PersistableEvents.late(tx("t2", "c", 0, "10", "PT"));

        assertThat(e.status()).isEqualTo(TransactionStatus.LATE);
        assertThat(e.risk()).isNull();
        assertThat(e.raiseAlert()).isFalse();
    }
}
