package io.github.jlmc.fraud.application.port.out;

import io.github.jlmc.fraud.validation.TransactionRiskRule;

import java.util.List;

/** Outbound port: where the risk rules come from (ServiceLoader today, anything tomorrow). Rules are already configured. */
public interface RiskRuleProvider {

    List<TransactionRiskRule> rules();
}
