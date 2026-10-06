package io.github.jlmc.fraud.validation;

public enum RiskLevel {
    LOW, MEDIUM, HIGH;

    /** Maps a 0..100 score to a level: &lt;40 LOW, &lt;70 MEDIUM, otherwise HIGH. */
    public static RiskLevel fromScore(int score) {
        if (score < 40) {
            return LOW;
        }
        return score < 70 ? MEDIUM : HIGH;
    }
}
