package io.github.jlmc.fraud.adapter.out.persistence;

import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.port.out.PersistenceException;
import io.github.jlmc.fraud.application.port.out.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Bounded batching with retry. No Flink types: it is exercised directly in unit tests.
 *
 * <p>Backpressure by construction: the buffer holds at most {@code batchSize} events and every call blocks (flush,
 * retry, backoff) on the caller's thread, which in Flink is the task thread. While it is blocked the task reads no
 * more input, its network buffers fill up and the slowdown propagates upstream to the Kafka source. Nothing is
 * queued without bound.
 *
 * <p>Outcomes of a batch: written; retried while the failure is systemic until the time budget is spent and then
 * thrown (never dropped); or, if the database rejects records, split so that only the offending records are skipped.
 */
public final class BatchedPersister {

    /** Sleeps; replaced in tests so that backoff does not slow them down. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    private static final Logger LOG = LoggerFactory.getLogger(BatchedPersister.class);

    private final TransactionRepository repository;
    private final int batchSize;
    private final RetryPolicy retry;
    private final Sleeper sleeper;
    private final LongSupplier nanoClock;
    private final PersistenceMetrics metrics;
    private final List<PersistableEvent> buffer = new ArrayList<>();

    public BatchedPersister(TransactionRepository repository, int batchSize, RetryPolicy retry,
                            Sleeper sleeper, LongSupplier nanoClock, PersistenceMetrics metrics) {
        this.repository = repository;
        this.batchSize = batchSize;
        this.retry = retry;
        this.sleeper = sleeper;
        this.nanoClock = nanoClock;
        this.metrics = metrics;
    }

    public void add(PersistableEvent event) {
        buffer.add(event);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    public boolean hasPending() {
        return !buffer.isEmpty();
    }

    /** Persists everything buffered. Returns only when it is durable in the database (or throws). */
    public void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        List<PersistableEvent> batch = List.copyOf(buffer);
        save(batch);
        buffer.clear(); // only after success: a thrown exception keeps the events for the failure path
    }

    private void save(List<PersistableEvent> batch) {
        long started = nanoClock.getAsLong();
        int attempt = 0;
        while (true) {
            try {
                long begin = nanoClock.getAsLong();
                repository.saveAll(batch);
                metrics.batchWritten(batch.size(), (nanoClock.getAsLong() - begin) / 1_000_000);
                return;
            } catch (PersistenceException e) {
                if (e.isRecordRejected()) {
                    isolateRejectedRecords(batch, e);
                    return;
                }
                Duration elapsed = Duration.ofNanos(nanoClock.getAsLong() - started);
                if (elapsed.compareTo(retry.maxElapsed()) >= 0) {
                    throw PersistenceException.systemic("Giving up after " + elapsed.toSeconds() + "s and " + attempt
                            + " retries, " + batch.size() + " events not persisted: " + e.getMessage(), e);
                }
                attempt++;
                metrics.retried();
                Duration backoff = retry.backoff(attempt);
                LOG.warn("Persistence failed (attempt {}), retrying in {} ms: {}", attempt, backoff.toMillis(), e.getMessage());
                pause(backoff, e);
            }
        }
    }

    /** The database refused something in this batch: find out what, persist the rest, skip only the culprits. */
    private void isolateRejectedRecords(List<PersistableEvent> batch, PersistenceException cause) {
        if (batch.size() == 1) {
            metrics.recordSkipped();
            LOG.error("Skipping a record the database rejects permanently: {} (reason: {})", batch.get(0), cause.getMessage());
            return;
        }
        for (PersistableEvent event : batch) {
            save(List.of(event));
        }
    }

    private void pause(Duration backoff, PersistenceException cause) {
        try {
            sleeper.sleep(backoff);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw PersistenceException.systemic("Interrupted while waiting to retry", cause);
        }
    }
}
