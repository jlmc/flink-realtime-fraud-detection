package io.github.jlmc.fraud.it.support;

import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.model.TransactionStatus;
import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;
import io.github.jlmc.fraud.validation.Transaction;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public final class Events {

    public static final Instant T0 = Instant.parse("2026-10-06T13:00:00Z");

    private Events() {
    }

    public static Transaction tx(String id) {
        return new Transaction(id, "customer-42", "merchant-10", new BigDecimal("950.00"), "EUR", "PT", T0);
    }

    public static PersistableEvent processed(String id, int score) {
        Transaction t = tx(id);
        RiskResult risk = new RiskResult(id, t.customerId(), score, RiskLevel.fromScore(score),
                score > 0 ? List.of("HIGH_TRANSACTION_VELOCITY") : List.of(), t.timestamp());
        return new PersistableEvent(t, TransactionStatus.PROCESSED, risk, risk.riskLevel() == RiskLevel.HIGH);
    }

    public static PersistableEvent late(String id) {
        return new PersistableEvent(tx(id), TransactionStatus.LATE, null, false);
    }
}
