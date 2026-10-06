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
import java.util.Properties;

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

    private final String url;
    private final Properties properties = new Properties();
    private Connection connection;

    public JdbcTransactionRepository(String url, String user, String password) {
        this.url = url;
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("ApplicationName", "fraud-flink-job");
        properties.setProperty("connectTimeout", "5");     // seconds
        properties.setProperty("socketTimeout", "30");     // a hung database must not block the task forever
        properties.setProperty("tcpKeepAlive", "true");
        properties.setProperty("reWriteBatchedInserts", "true");
    }

    @Override
    public void saveAll(List<PersistableEvent> batch) {
        try {
            Connection c = connection();
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
            }
        } catch (SQLException e) {
            rollbackAndReset();
            throw SqlErrors.classify("Could not persist a batch of " + batch.size(), e);
        } catch (JsonProcessingException e) {
            rollbackAndReset();
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

    private Connection connection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            Connection c = new org.postgresql.Driver().connect(url, properties);
            if (c == null) {
                throw new SQLException("Not a PostgreSQL JDBC url: " + url, "08001");
            }
            c.setAutoCommit(false);
            connection = c;
        }
        return connection;
    }

    /** After any failure the connection state is unknown: discard it, the next attempt opens a fresh one. */
    private void rollbackAndReset() {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {
                // the connection is probably broken, closing it below is what matters
            }
            closeQuietly();
        }
    }

    private void closeQuietly() {
        try {
            connection.close();
        } catch (SQLException ignored) {
            // nothing useful to do
        } finally {
            connection = null;
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            closeQuietly();
        }
    }
}
