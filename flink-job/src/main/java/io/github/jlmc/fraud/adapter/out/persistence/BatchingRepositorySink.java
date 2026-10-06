package io.github.jlmc.fraud.adapter.out.persistence;

import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.port.out.TransactionRepository;
import io.github.jlmc.fraud.application.port.out.TransactionRepositoryFactory;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;

/**
 * Flink Sink V2 around {@link BatchedPersister}. It adds nothing but the Flink plumbing: metrics, a periodic flush so
 * a quiet stream does not leave events waiting, and a flush when a checkpoint barrier arrives.
 *
 * <p>Delivery guarantee to the database: AT-LEAST-ONCE. {@link #flush} runs before a checkpoint completes, so every
 * event received before the barrier is durable in PostgreSQL by then. After a failure Flink replays from the last
 * checkpoint and some events are written again; the repository's idempotent inserts turn those replays into no-ops.
 * Checkpointing alone does NOT make this exactly-once: a database commit and a Flink checkpoint are two separate
 * atomic actions that cannot be made one without a two-phase-commit sink.
 */
public class BatchingRepositorySink implements Sink<PersistableEvent> {

    private final TransactionRepositoryFactory repositoryFactory;
    private final int batchSize;
    private final long flushIntervalMillis;
    private final RetryPolicy retryPolicy;

    public BatchingRepositorySink(TransactionRepositoryFactory repositoryFactory, int batchSize,
                                  long flushIntervalMillis, RetryPolicy retryPolicy) {
        this.repositoryFactory = repositoryFactory;
        this.batchSize = batchSize;
        this.flushIntervalMillis = flushIntervalMillis;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public SinkWriter<PersistableEvent> createWriter(WriterInitContext context) throws IOException {
        return new Writer(repositoryFactory.create(), context);
    }

    private final class Writer implements SinkWriter<PersistableEvent> {

        private final TransactionRepository repository;
        private final BatchedPersister persister;
        private final ProcessingTimeService timers;
        private volatile boolean closed;
        private ScheduledFuture<?> timer;

        Writer(TransactionRepository repository, WriterInitContext context) {
            this.repository = repository;
            this.timers = context.getProcessingTimeService();
            this.persister = new BatchedPersister(repository, batchSize, retryPolicy,
                    d -> Thread.sleep(d.toMillis()), System::nanoTime, new FlinkMetrics(context.metricGroup()));
            scheduleFlush();
        }

        private void scheduleFlush() {
            timer = timers.registerTimer(timers.getCurrentProcessingTime() + flushIntervalMillis, time -> {
                if (!closed) {
                    persister.flush();
                    scheduleFlush();
                }
            });
        }

        @Override
        public void write(PersistableEvent event, Context context) {
            persister.add(event);
        }

        @Override
        public void flush(boolean endOfInput) {
            persister.flush();
        }

        @Override
        public void close() {
            closed = true;
            if (timer != null) {
                timer.cancel(false);
            }
            repository.close();
        }
    }

    private static final class FlinkMetrics implements PersistenceMetrics {

        private final Counter rows;
        private final Counter batches;
        private final Counter retries;
        private final Counter skipped;
        private volatile long lastLatencyMillis;

        FlinkMetrics(SinkWriterMetricGroup group) {
            this.rows = group.counter("postgres_rows_written");
            this.batches = group.counter("postgres_batches_written");
            this.retries = group.counter("postgres_retries");
            this.skipped = group.counter("postgres_records_skipped");
            group.gauge("postgres_last_batch_latency_ms", () -> lastLatencyMillis);
        }

        @Override
        public void batchWritten(int count, long latencyMillis) {
            rows.inc(count);
            batches.inc();
            lastLatencyMillis = latencyMillis;
        }

        @Override
        public void retried() {
            retries.inc();
        }

        @Override
        public void recordSkipped() {
            skipped.inc();
        }
    }
}
