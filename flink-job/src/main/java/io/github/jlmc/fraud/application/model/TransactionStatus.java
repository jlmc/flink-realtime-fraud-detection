package io.github.jlmc.fraud.application.model;

public enum TransactionStatus {
    /** Evaluated for risk in event-time order. */
    PROCESSED,
    /** Arrived behind the watermark: stored, but not risk-evaluated. */
    LATE
}
