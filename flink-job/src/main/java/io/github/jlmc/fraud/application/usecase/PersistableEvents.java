package io.github.jlmc.fraud.application.usecase;

import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.model.RiskOutcome;
import io.github.jlmc.fraud.application.model.TransactionStatus;
import io.github.jlmc.fraud.domain.risk.FraudAlertPolicy;
import io.github.jlmc.fraud.validation.Transaction;

/** Builds what gets persisted from the pipeline results. */
public final class PersistableEvents {

    private PersistableEvents() {
    }

    public static PersistableEvent processed(RiskOutcome outcome) {
        return new PersistableEvent(outcome.transaction(), TransactionStatus.PROCESSED, outcome.result(),
                FraudAlertPolicy.shouldAlert(outcome.result().riskLevel()));
    }

    public static PersistableEvent late(Transaction transaction) {
        return new PersistableEvent(transaction, TransactionStatus.LATE, null, false);
    }
}
