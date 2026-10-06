package io.github.jlmc.fraud.adapter.out.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * One HikariCP pool per distinct {@link PoolConfig} in this JVM, shared by every sink subtask running in the
 * TaskManager and closed when the last one lets go (reference counting).
 *
 * <p>Why shared rather than one pool per subtask: the writers are single-threaded, so a private pool would only add
 * connection health management. A shared pool adds the property that matters here: PostgreSQL never sees more than
 * {@code maxPoolSize} connections from this TaskManager no matter how high the parallelism is, and writers beyond
 * that wait their turn instead of opening more connections.
 *
 * <p>State is static, hence per classloader: each Flink job (and each restart) has its own user-code classloader, so
 * pools never leak between jobs. Subtasks always call {@link Lease#close()} from the writer's {@code close()}.
 */
public final class PooledDataSources {

    /** A counted reference to a shared pool. Closing is idempotent. */
    public interface Lease extends AutoCloseable {
        DataSource dataSource();

        @Override
        void close();
    }

    private static final class Entry {
        final HikariDataSource dataSource;
        int references;

        Entry(HikariDataSource dataSource) {
            this.dataSource = dataSource;
        }
    }

    private static final Map<PoolConfig, Entry> POOLS = new HashMap<>();

    private PooledDataSources() {
    }

    public static synchronized Lease acquire(PoolConfig config) {
        Entry entry = POOLS.computeIfAbsent(config, c -> new Entry(create(c)));
        entry.references++;
        return new PoolLease(config, entry);
    }

    private static final class PoolLease implements Lease {
        private final PoolConfig config;
        private final Entry entry;
        private boolean closed;

        PoolLease(PoolConfig config, Entry entry) {
            this.config = config;
            this.entry = entry;
        }

        @Override
        public DataSource dataSource() {
            return entry.dataSource;
        }

        @Override
        public void close() {
            synchronized (PooledDataSources.class) {
                if (closed) {
                    return;
                }
                closed = true;
                entry.references--;
                if (entry.references == 0) {
                    POOLS.remove(config, entry);
                    entry.dataSource.close();
                }
            }
        }
    }

    /** Number of live pools in this JVM (diagnostics and tests). */
    static synchronized int livePools() {
        return POOLS.size();
    }

    private static HikariDataSource create(PoolConfig c) {
        // Hand Hikari a ready PGSimpleDataSource: going through DriverManager (jdbcUrl) would not find the driver from
        // Flink's user-code classloader.
        PGSimpleDataSource postgres = new PGSimpleDataSource();
        postgres.setUrl(c.url());
        postgres.setUser(c.user());
        postgres.setPassword(c.password());
        postgres.setApplicationName("fraud-flink-job");
        postgres.setLoginTimeout(5);            // seconds, the whole login handshake (connectTimeout only covers the TCP connect)
        postgres.setConnectTimeout(5);          // seconds, TCP connect
        postgres.setSocketTimeout(30);          // seconds; HikariCP's "Rapid Recovery" advice: at least 30 s, so a dead peer is noticed
        postgres.setTcpKeepAlive(true);
        postgres.setReWriteBatchedInserts(true);
        // Server-side safety net for every pooled session (see docs/decisions/0001):
        //  statement_timeout                    the SERVER cancels a runaway statement; below socketTimeout so that the clean
        //                                       SQLSTATE 57014 arrives before the client gives up on the socket
        //  idle_in_transaction_session_timeout  the server kills a session left inside a transaction. Pooled connections run with
        //                                       autoCommit=false, so a transaction left open would otherwise hold locks forever
        //  lock_timeout                         never wait unbounded on a lock
        // (A TimeZone option here would be ignored: the driver sets the session time zone itself. It does not matter for
        // correctness, instants are written as UTC OffsetDateTime.)
        postgres.setOptions("-c statement_timeout=20000 -c idle_in_transaction_session_timeout=60000 -c lock_timeout=10000");

        HikariConfig hikari = new HikariConfig();
        hikari.setDataSource(postgres);
        hikari.setPoolName("fraud-postgres");
        hikari.setMaximumPoolSize(c.maxPoolSize());
        // minimumIdle is deliberately NOT set: HikariCP recommends a fixed-size pool (minimumIdle = maximumPoolSize) for the
        // best performance and responsiveness. A streaming sink keeps its connections busy anyway.
        hikari.setAutoCommit(false);            // every batch is an explicit transaction; Hikari rolls back whatever is left open on return
        hikari.setTransactionIsolation("TRANSACTION_READ_COMMITTED"); // explicit rather than "whatever the driver defaults to"
        hikari.setConnectionTimeout(c.connectionTimeoutMs());
        hikari.setValidationTimeout(Duration.ofSeconds(5).toMillis());
        hikari.setKeepaliveTime(Duration.ofMinutes(2).toMillis());   // minutes range, below maxLifetime and below typical idle cut-offs
        hikari.setMaxLifetime(Duration.ofMinutes(30).toMillis());
        hikari.setInitializationFailTimeout(-1); // do not fail at start when the database is down: the persister retries
        hikari.setRegisterMbeans(false);
        return new HikariDataSource(hikari);
    }
}
