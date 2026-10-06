package io.github.jlmc.fraud.application.port.out;

/**
 * Failure while persisting. Two kinds, because they need opposite reactions:
 * <ul>
 *   <li><b>systemic</b> (default): the database is unreachable, overloaded, or misconfigured. Retrying later may
 *       work and dropping data is never acceptable, so the caller retries and, in the end, fails the job.</li>
 *   <li><b>record rejected</b>: the database refuses this particular record (too long, null in a NOT NULL column).
 *       Retrying can never help, and failing the job would restart it forever on the same record.</li>
 * </ul>
 */
public class PersistenceException extends RuntimeException {

    private final boolean recordRejected;

    private PersistenceException(String message, Throwable cause, boolean recordRejected) {
        super(message, cause);
        this.recordRejected = recordRejected;
    }

    public static PersistenceException systemic(String message, Throwable cause) {
        return new PersistenceException(message, cause, false);
    }

    public static PersistenceException recordRejected(String message, Throwable cause) {
        return new PersistenceException(message, cause, true);
    }

    public boolean isRecordRejected() {
        return recordRejected;
    }
}
