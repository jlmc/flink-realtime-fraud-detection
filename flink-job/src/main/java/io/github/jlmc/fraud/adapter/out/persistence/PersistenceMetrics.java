package io.github.jlmc.fraud.adapter.out.persistence;

/** What the persister reports. Implemented with Flink metrics in production and with a recorder in tests. */
public interface PersistenceMetrics {

    void batchWritten(int rows, long latencyMillis);

    /** A retry is about to happen after a systemic failure. */
    void retried();

    /** A record the database rejected permanently was skipped. */
    void recordSkipped();

    PersistenceMetrics NONE = new PersistenceMetrics() {
        @Override
        public void batchWritten(int rows, long latencyMillis) {
        }

        @Override
        public void retried() {
        }

        @Override
        public void recordSkipped() {
        }
    };
}
