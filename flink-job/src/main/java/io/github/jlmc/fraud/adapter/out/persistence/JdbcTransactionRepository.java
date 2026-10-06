package io.github.jlmc.fraud.adapter.out.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.port.out.PersistenceException;
import io.github.jlmc.fraud.application.port.out.TransactionRepository;
import io.github.jlmc.fraud.validation.RiskResult;
import io.github.jlmc.fraud.validation.Transaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * PostgreSQL implementation of the repository port.
 *
 * <p>Idempotency: every insert is {@code ON CONFLICT (transaction_id) DO NOTHING}, so replaying a transaction after a
 * recovery leaves exactly one row per table. A whole batch is one database transaction (all or nothing), which makes
 * retrying a failed batch safe. One connection per sink subtask, opened lazily and reopened after a failure.
 *
 * <p>The PostgreSQL driver is instantiated directly, not through {@code DriverManager}: inside Flink's user-code
 * classloader the driver is not visible to DriverManager's classloader checks.
 */
public final class JdbcTransactionRepository implements TransactionRepository {

    static final String INSERT_TRANSACTION = """
            INSERT INTO transactions (transaction_id, customer_id, merchant_id, amount, currency, country, event_time, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_id) DO NOTHING""";

    static final String INSERT_RISK_SCORE = """
            INSERT INTO risk_scores (transaction_id, customer_id, risk_score, risk_level, reasons, event_time)
            VALUES (?, ?, ?, ?, ?::jsonb, ?)
            ON CONFLICT (transaction_id) DO NOTHING""";

    static final String INSERT_FRAUD_ALERT = """
            INSERT INTO fraud_alerts (transaction_id, customer_id, risk_score, risk_level, reasons, event_time)
            VALUES (?, ?, ?, ?, ?::jsonb, ?)
            ON CONFLICT (transaction_id) DO NOTHING""";

    private static final ObjectMapper JSON = new ObjectMapper();

    private final PooledDataSources.Lease pool;

    /** Takes ownership of the lease: {@link #close()} gives the pool reference back. */
    public JdbcTransactionRepository(PooledDataSources.Lease pool) {
        this.pool = pool;
    }

    @Override
    public void saveAll(List<PersistableEvent> batch) {
        // The connection is borrowed for this batch only; closing it returns it to the pool. Hikari rolls back
        // anything uncommitted and evicts connections that fail with connection-level errors, so no reconnect logic
        // is needed here.
        try (Connection c = pool.dataSource().getConnection()) {
            try (PreparedStatement transactions = c.prepareStatement(INSERT_TRANSACTION);
                 PreparedStatement scores = c.prepareStatement(INSERT_RISK_SCORE);
                 PreparedStatement alerts = c.prepareStatement(INSERT_FRAUD_ALERT)) {
                boolean anyScore = false;
                boolean anyAlert = false;
                for (PersistableEvent event : batch) {
                    bind(transactions, event);
                    transactions.addBatch();
                    if (event.risk() != null) {
                        bind(scores, event.transaction(), event.risk());
                        scores.addBatch();
                        anyScore = true;
                    }
                    if (event.raiseAlert()) {
                        bind(alerts, event.transaction(), event.risk());
                        alerts.addBatch();
                        anyAlert = true;
                    }
                }
                transactions.executeBatch();
                if (anyScore) {
                    scores.executeBatch();
                }
                if (anyAlert) {
                    alerts.executeBatch();
                }
                c.commit();
            } catch (SQLException | JsonProcessingException e) {
                rollbackQuietly(c);
                throw e;
            }
        } catch (SQLException e) {
            throw SqlErrors.classify("Could not persist a batch of " + batch.size(), e);
        } catch (JsonProcessingException e) {
            throw PersistenceException.recordRejected("Cannot serialise risk reasons", e);
        }
    }

    private static void bind(PreparedStatement ps, PersistableEvent event) throws SQLException {
        Transaction t = event.transaction();
        ps.setString(1, t.transactionId());
        ps.setString(2, t.customerId());
        ps.setString(3, t.merchantId());
        ps.setBigDecimal(4, t.amount());
        ps.setString(5, t.currency());
        ps.setString(6, t.country());
        ps.setObject(7, OffsetDateTime.ofInstant(t.timestamp(), ZoneOffset.UTC));
        ps.setString(8, event.status().name());
    }

    private static void bind(PreparedStatement ps, Transaction t, RiskResult r) throws SQLException, JsonProcessingException {
        ps.setString(1, t.transactionId());
        ps.setString(2, t.customerId());
        ps.setInt(3, r.riskScore());
        ps.setString(4, r.riskLevel().name());
        ps.setString(5, JSON.writeValueAsString(r.reasons()));
        ps.setObject(6, OffsetDateTime.ofInstant(r.timestamp(), ZoneOffset.UTC));
    }

    private static void rollbackQuietly(Connection c) {
        try {
            c.rollback();
        } catch (SQLException ignored) {
            // the connection is probably broken; the pool will evict it
        }
    }

    @Override
    public void close() {
        pool.close();
    }
}
