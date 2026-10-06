package io.github.jlmc.fraud.application.port.out;

import io.github.jlmc.fraud.validation.TransactionValidationRule;

import java.util.List;

/** Outbound port: where the validation rules come from (ServiceLoader today, anything tomorrow). */
public interface ValidationRuleProvider {

    List<TransactionValidationRule> rules();
}
