package io.github.jlmc.fraud.it;

import io.github.jlmc.fraud.adapter.out.persistence.BatchedPersister;
import io.github.jlmc.fraud.adapter.out.persistence.JdbcTransactionRepository;
import io.github.jlmc.fraud.adapter.out.persistence.PersistenceMetrics;
import io.github.jlmc.fraud.adapter.out.persistence.PoolConfig;
import io.github.jlmc.fraud.adapter.out.persistence.PooledDataSources;
import io.github.jlmc.fraud.adapter.out.persistence.RetryPolicy;
import io.github.jlmc.fraud.it.support.PostgresFixture;
import io.github.jlmc.fraud.it.support.PostgresFixture.Database;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.jlmc.fraud.it.support.Events.processed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The shared HikariCP pool against a real PostgreSQL. */
class PooledDataSourcesIT {

    private Database db;

    @BeforeEach
    void setUp() {
        db = PostgresFixture.newDatabase();
    }

    private PoolConfig pool(int size, long connectionTimeoutMs) {
        return new PoolConfig(db.jdbcUrl(), db.user(), db.password(), size, connectionTimeoutMs);
    }

    /** Connections this database currently sees from the job's pool. */
    private int jobConnections() {
        try (Connection c = PostgresFixture.admin(); var s = c.createStatement();
             var rs = s.executeQuery("select count(*) from pg_stat_activity where datname = '" + db.name()
                     + "' and application_name = 'fraud-flink-job'")) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void theExtraWriterWaitsAndTimesOutInsteadOfOpeningAThirdConnection() throws Exception {
        try (var lease = PooledDataSources.acquire(pool(2, 600));
             Connection first = lease.dataSource().getConnection();
             Connection second = lease.dataSource().getConnection()) {

            assertThat(jobConnections()).isEqualTo(2);

            long started = System.nanoTime();
            assertThatThrownBy(() -> lease.dataSource().getConnection()).isInstanceOf(SQLTransientConnectionException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isGreaterThanOrEqualTo(Duration.ofMillis(500));

            assertThat(jobConnections()).as("still capped").isEqualTo(2);
        }
    }

    @Test
    void manyConcurrentWritersShareSmallPoolAndNeverExceedTheCap() throws Exception {
        int writers = 8;
        int batchesEach = 15;
        AtomicInteger maxSeen = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(writers + 1);
        try {
            var watcher = executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    maxSeen.accumulateAndGet(jobConnections(), Math::max);
                    Thread.sleep(20);
                }
                return null;
            });

            List<Future<?>> work = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                int writer = w;
                work.add(executor.submit(() -> {
                    // every writer owns a repository, like every sink subtask; they all lease the same pool
                    try (var repository = new JdbcTransactionRepository(PooledDataSources.acquire(pool(2, 30_000)))) {
                        for (int b = 0; b < batchesEach; b++) {
                            repository.saveAll(List.of(processed("w" + writer + "-b" + b, 10)));
                        }
                    }
                }));
            }
            for (Future<?> f : work) {
                f.get(60, TimeUnit.SECONDS);
            }
            watcher.cancel(true);
        } finally {
            executor.shutdownNow();
        }

        assertThat(db.count("transactions")).isEqualTo((long) writers * batchesEach);
        assertThat(maxSeen.get()).as("8 writers, pool of 2").isBetween(1, 2);
    }

    @Test
    void theWriterRecoversWhenTheServerDropsEveryConnection() throws Exception {
        AtomicInteger retries = new AtomicInteger();
        PersistenceMetrics metrics = new PersistenceMetrics() {
            @Override
            public void batchWritten(int rows, long latencyMillis) {
            }

            @Override
            public void retried() {
                retries.incrementAndGet();
            }

            @Override
            public void recordSkipped() {
            }
        };
        try (var repository = new JdbcTransactionRepository(PooledDataSources.acquire(pool(2, 2000)))) {
            var persister = new BatchedPersister(repository, 10,
                    new RetryPolicy(Duration.ofSeconds(30), Duration.ofMillis(100), Duration.ofMillis(500), 2.0),
                    d -> Thread.sleep(d.toMillis()), System::nanoTime, metrics);

            persister.add(processed("before", 10));
            persister.flush();
            assertThat(jobConnections()).isPositive();

            // what a database restart does to a pool: every pooled connection is suddenly dead
            try (Connection admin = PostgresFixture.admin(); var s = admin.createStatement()) {
                s.execute("select pg_terminate_backend(pid) from pg_stat_activity where datname = '" + db.name()
                        + "' and application_name = 'fraud-flink-job'");
            }

            persister.add(processed("after", 10));
            persister.flush();   // must not throw: the dead connection is evicted and replaced
        }

        assertThat(db.count("transactions")).isEqualTo(2);
    }

    @Test
    void nothingIsLeftOpenOnceEveryWriterHasClosed() throws Exception {
        var a = new JdbcTransactionRepository(PooledDataSources.acquire(pool(2, 2000)));
        var b = new JdbcTransactionRepository(PooledDataSources.acquire(pool(2, 2000)));
        a.saveAll(List.of(processed("a", 10)));
        b.saveAll(List.of(processed("b", 10)));
        assertThat(jobConnections()).isPositive();

        a.close();
        assertThat(jobConnections()).as("one lease is still using the pool").isPositive();

        b.close();
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (jobConnections() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertThat(jobConnections()).as("the pool closed with its last lease").isZero();
    }

    @Test
    void pooledConnectionsStartWithAutoCommitOffAndAnExplicitIsolationLevel() throws Exception {
        try (var lease = PooledDataSources.acquire(pool(2, 2000)); Connection c = lease.dataSource().getConnection()) {
            assertThat(c.getAutoCommit()).isFalse();
            assertThat(c.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
        }
    }

    @Test
    void aConnectionReturnedWithAnOpenTransactionIsRolledBackNotLeaked() throws Exception {
        try (var lease = PooledDataSources.acquire(pool(1, 2000))) {
            try (Connection c = lease.dataSource().getConnection(); var s = c.createStatement()) {
                s.execute("insert into transactions (transaction_id, customer_id, merchant_id, amount, currency, event_time) "
                        + "values ('forgotten', 'c', 'm', 1, 'EUR', now())");
                // no commit, no rollback: a caller bug. With autoCommit=false the pool must clean up on return.
            }

            assertThat(db.count("transactions")).as("the forgotten insert must not become visible").isZero();
            try (Connection again = lease.dataSource().getConnection(); var s = again.createStatement();
                 var rs = s.executeQuery("select count(*) from transactions")) {
                rs.next();
                assertThat(rs.getLong(1)).as("same pooled connection, clean state").isZero();
            }
        }
    }

    @Test
    void everyPooledSessionCarriesTheServerSideSafetySettings() throws Exception {
        try (var lease = PooledDataSources.acquire(pool(1, 2000)); Connection c = lease.dataSource().getConnection(); var s = c.createStatement()) {
            assertThat(show(s, "statement_timeout")).isEqualTo("20s");
            assertThat(show(s, "idle_in_transaction_session_timeout")).isEqualTo("1min");
            assertThat(show(s, "lock_timeout")).isEqualTo("10s");
        }
    }

    private static String show(java.sql.Statement s, String setting) throws SQLException {
        try (var rs = s.executeQuery("show " + setting)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void thePoolIsFixedSizeAsHikariRecommends() throws Exception {
        try (var lease = PooledDataSources.acquire(pool(3, 2000))) {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (jobConnections() < 3 && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertThat(jobConnections()).as("all connections are opened up front, nothing is created under load").isEqualTo(3);
        }
    }
}
