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
        postgres.setConnectTimeout(5);          // seconds, for the TCP connect
        postgres.setSocketTimeout(30);          // seconds: a hung database must not block a writer forever
        postgres.setTcpKeepAlive(true);
        postgres.setReWriteBatchedInserts(true);

        HikariConfig hikari = new HikariConfig();
        hikari.setDataSource(postgres);
        hikari.setPoolName("fraud-postgres");
        hikari.setMaximumPoolSize(c.maxPoolSize());
        hikari.setMinimumIdle(1);
        hikari.setAutoCommit(false);            // every batch is an explicit transaction
        hikari.setConnectionTimeout(c.connectionTimeoutMs());
        hikari.setValidationTimeout(Duration.ofSeconds(5).toMillis());
        hikari.setKeepaliveTime(Duration.ofMinutes(2).toMillis());   // below typical firewall/proxy idle cut-offs
        hikari.setMaxLifetime(Duration.ofMinutes(30).toMillis());
        hikari.setInitializationFailTimeout(-1); // do not fail at start when the database is down: the persister retries
        hikari.setRegisterMbeans(false);
        return new HikariDataSource(hikari);
    }
}
