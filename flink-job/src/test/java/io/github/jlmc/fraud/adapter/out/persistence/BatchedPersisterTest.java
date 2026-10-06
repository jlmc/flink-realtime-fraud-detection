package io.github.jlmc.fraud.adapter.out.persistence;

import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.model.TransactionStatus;
import io.github.jlmc.fraud.application.port.out.PersistenceException;
import io.github.jlmc.fraud.testsupport.InMemoryRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.jlmc.fraud.testsupport.Transactions.tx;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchedPersisterTest {

    /** Virtual time: sleeping advances the clock instead of waiting. */
    private final AtomicLong nanos = new AtomicLong();
    private final List<Duration> sleeps = new ArrayList<>();
    private final InMemoryRepository repository = new InMemoryRepository();
    private final Recorder metrics = new Recorder();
    private final RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(10), Duration.ofSeconds(1), Duration.ofSeconds(4), 2.0);

    private BatchedPersister persister(int batchSize) {
        return new BatchedPersister(repository, batchSize, retry, d -> {
            sleeps.add(d);
            nanos.addAndGet(d.toNanos());
        }, nanos::get, metrics);
    }

    private static PersistableEvent event(String id) {
        return new PersistableEvent(tx(id, "c", 0, "10", "PT"), TransactionStatus.LATE, null, false);
    }

    @BeforeEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @AfterEach
    void clearInterruptFlagAfter() {
        Thread.interrupted();
    }

    private static final class Recorder implements PersistenceMetrics {
        int rows;
        int batches;
        int retries;
        int skipped;

        @Override
        public void batchWritten(int count, long latencyMillis) {
            rows += count;
            batches++;
        }

        @Override
        public void retried() {
            retries++;
        }

        @Override
        public void recordSkipped() {
            skipped++;
        }
    }

    @Test
    void writesWhenTheBatchIsFullAndNeverBuffersMoreThanTheBatchSize() {
        BatchedPersister p = persister(3);

        for (int i = 1; i <= 7; i++) {
            p.add(event("t" + i));
        }

        assertThat(repository.batchSizes).containsExactly(3, 3);
        assertThat(repository.stored).hasSize(6);
        assertThat(p.hasPending()).isTrue();

        p.flush();
        assertThat(repository.batchSizes).containsExactly(3, 3, 1);
        assertThat(repository.stored).hasSize(7);
        assertThat(p.hasPending()).isFalse();
    }

    @Test
    void flushWithNothingPendingDoesNotTouchTheDatabase() {
        persister(3).flush();

        assertThat(repository.calls).isZero();
    }

    @Test
    void replayingTheSameEventsLeavesOneRowEach() {
        BatchedPersister p = persister(2);
        p.add(event("t1"));
        p.add(event("t2"));
        p.add(event("t1")); // replay after a recovery
        p.add(event("t2"));
        p.flush();

        assertThat(repository.stored).hasSize(2);
    }

    @Test
    void aTransientOutageIsRetriedWithExponentialBackoffAndThenSucceeds() {
        int[] failuresLeft = {3};
        repository.failure = batch -> failuresLeft[0]-- > 0 ? PersistenceException.systemic("connection refused", null) : null;
        BatchedPersister p = persister(10);
        p.add(event("t1"));

        p.flush();

        assertThat(repository.stored).containsKey("t1");
        assertThat(sleeps).containsExactly(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4));
        assertThat(metrics.retries).isEqualTo(3);
        assertThat(p.hasPending()).isFalse();
    }

    @Test
    void aProlongedOutageIsNeverDroppedItFailsAfterTheBudgetAndKeepsTheEvents() {
        repository.failure = batch -> PersistenceException.systemic("database down", null);
        BatchedPersister p = persister(10);
        p.add(event("t1"));

        assertThatThrownBy(p::flush)
                .isInstanceOf(PersistenceException.class)
                .hasMessageContaining("Giving up")
                .hasMessageContaining("1 events not persisted");

        assertThat(p.hasPending()).as("events must not be lost when giving up").isTrue();
        assertThat(repository.stored).isEmpty();
        assertThat(sleeps.stream().mapToLong(Duration::toSeconds).sum()).isGreaterThanOrEqualTo(10);
    }

    @Test
    void aRejectedRecordIsIsolatedAndOnlyThatRecordIsSkipped() {
        repository.failure = batch -> batch.stream().anyMatch(e -> e.transaction().transactionId().equals("poison"))
                ? PersistenceException.recordRejected("value too long", null) : null;
        BatchedPersister p = persister(10);
        p.add(event("a"));
        p.add(event("poison"));
        p.add(event("b"));

        p.flush();

        assertThat(repository.stored).containsOnlyKeys("a", "b");
        assertThat(metrics.skipped).isEqualTo(1);
        assertThat(sleeps).as("a rejected record is not retried").isEmpty();
        assertThat(p.hasPending()).isFalse();
    }

    @Test
    void aSystemicFailureWhileIsolatingStillFailsInsteadOfSkipping() {
        repository.failure = batch -> batch.size() > 1
                ? PersistenceException.recordRejected("something in here is bad", null)
                : PersistenceException.systemic("database went down meanwhile", null);
        BatchedPersister p = persister(10);
        p.add(event("a"));
        p.add(event("b"));

        assertThatThrownBy(p::flush).isInstanceOf(PersistenceException.class).hasMessageContaining("Giving up");
        assertThat(metrics.skipped).isZero();
        assertThat(p.hasPending()).isTrue();
    }

    @Test
    void reportsBatchesAndRows() {
        BatchedPersister p = persister(2);
        p.add(event("a"));
        p.add(event("b"));
        p.add(event("c"));
        p.flush();

        assertThat(metrics.rows).isEqualTo(3);
        assertThat(metrics.batches).isEqualTo(2);
    }

    @Test
    void interruptionWhileBackingOffStopsRetryingAndRestoresTheInterruptFlag() {
        repository.failure = batch -> PersistenceException.systemic("down", null);
        BatchedPersister p = new BatchedPersister(repository, 10, retry, d -> {
            throw new InterruptedException();
        }, nanos::get, metrics);
        p.add(event("a"));

        assertThatThrownBy(p::flush).isInstanceOf(PersistenceException.class).hasMessageContaining("Interrupted");
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }
}
