package io.github.jlmc.fraud.domain.risk;

import io.github.jlmc.fraud.validation.RiskLevel;
import io.github.jlmc.fraud.validation.RiskResult;

/**
 * A fraud alert is raised for HIGH risk only; LOW and MEDIUM results are recorded as scores. The threshold is the one
 * of {@link RiskLevel#fromScore(int)} (score &gt;= 70), so there is a single definition of "high risk".
 * Used both for the {@code fraud_alerts} table and for the alerts topic.
 */
public final class FraudAlertPolicy {

    private FraudAlertPolicy() {
    }

    public static boolean shouldAlert(RiskLevel level) {
        return level == RiskLevel.HIGH;
    }

    public static boolean shouldAlert(RiskResult result) {
        return shouldAlert(result.riskLevel());
    }
}
