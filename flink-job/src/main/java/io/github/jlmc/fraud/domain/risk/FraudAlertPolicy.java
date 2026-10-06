package io.github.jlmc.fraud.domain.risk;

import io.github.jlmc.fraud.validation.RiskLevel;

/** A fraud alert is raised for HIGH risk only; LOW and MEDIUM results are recorded as scores. */
public final class FraudAlertPolicy {

    private FraudAlertPolicy() {
    }

    public static boolean shouldAlert(RiskLevel level) {
        return level == RiskLevel.HIGH;
    }
}
