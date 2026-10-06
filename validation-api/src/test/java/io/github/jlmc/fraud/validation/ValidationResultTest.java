package io.github.jlmc.fraud.validation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidationResultTest {

    @Test
    void validHasNoViolations() {
        assertThat(ValidationResult.valid().isValid()).isTrue();
    }

    @Test
    void invalidCarriesViolation() {
        ValidationResult result = ValidationResult.invalid("X", "boom");
        assertThat(result.isValid()).isFalse();
        assertThat(result.violations()).containsExactly(new Violation("X", "boom"));
    }

    @Test
    void emptyViolationListIsValid() {
        assertThat(ValidationResult.invalid(java.util.List.of()).isValid()).isTrue();
    }

    @Test
    void riskScoreMustBeInRange() {
        assertThatThrownBy(() -> new RiskResult("t", "c", 101, RiskLevel.HIGH, java.util.List.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void riskLevelFromScore() {
        assertThat(RiskLevel.fromScore(0)).isEqualTo(RiskLevel.LOW);
        assertThat(RiskLevel.fromScore(39)).isEqualTo(RiskLevel.LOW);
        assertThat(RiskLevel.fromScore(40)).isEqualTo(RiskLevel.MEDIUM);
        assertThat(RiskLevel.fromScore(70)).isEqualTo(RiskLevel.HIGH);
    }
}
