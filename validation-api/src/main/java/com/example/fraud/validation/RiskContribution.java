package com.example.fraud.validation;

/**
 * Contribution of a single risk rule to the final score.
 *
 * @param score  points added to the aggregate score (0 if the rule did not trigger)
 * @param reason reason code, or {@code null} if the rule did not trigger
 */
public record RiskContribution(int score, String reason) {

    private static final RiskContribution NONE = new RiskContribution(0, null);

    public RiskContribution {
        if (score < 0 || score > 100) {
            throw new IllegalArgumentException("score must be within 0..100: " + score);
        }
    }

    public static RiskContribution none() {
        return NONE;
    }

    public static RiskContribution triggered(int score, String reason) {
        return new RiskContribution(score, reason);
    }

    public boolean isTriggered() {
        return reason != null;
    }
}
