package io.github.jlmc.fraud.adapter.out.persistence;

import java.io.Serializable;
import java.time.Duration;

/**
 * Exponential backoff with a total time budget.
 *
 * @param maxElapsed total time to keep retrying one batch before giving up (the job then fails and recovers from its
 *                   last checkpoint). Keep it below the checkpoint timeout so a database outage surfaces as a failure
 *                   rather than as a never-ending stuck checkpoint.
 */
public record RetryPolicy(Duration maxElapsed, Duration initialBackoff, Duration maxBackoff, double multiplier) implements Serializable {

    public static RetryPolicy defaults() {
        return new RetryPolicy(Duration.ofSeconds(60), Duration.ofMillis(200), Duration.ofSeconds(5), 2.0);
    }

    /** Delay before retry number {@code attempt} (1-based). */
    public Duration backoff(int attempt) {
        double millis = initialBackoff.toMillis() * Math.pow(multiplier, Math.max(0, attempt - 1));
        return Duration.ofMillis((long) Math.min(millis, maxBackoff.toMillis()));
    }
}
