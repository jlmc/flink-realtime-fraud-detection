package io.github.jlmc.fraud.adapter.out.persistence;

import io.github.jlmc.fraud.application.port.out.PersistenceException;

import java.sql.SQLException;

/**
 * Decides whether a SQL failure is about the record or about the system.
 *
 * <p>Only SQLSTATE classes 22 (data exception: value too long, bad numeric...) and 23 (integrity constraint: NOT NULL,
 * CHECK...) are about the record. Everything else, including 42 (undefined table: a missing migration) and 08/53/57
 * (connection, resources, shutdown), is systemic: treating a missing table as "bad record" would silently discard
 * every transaction.
 */
public final class SqlErrors {

    private SqlErrors() {
    }

    public static boolean isRecordRejected(SQLException e) {
        for (SQLException current = e; current != null; current = current.getNextException()) {
            String state = current.getSQLState();
            if (state != null && (state.startsWith("22") || state.startsWith("23"))) {
                return true;
            }
        }
        return false;
    }

    public static PersistenceException classify(String context, SQLException e) {
        return isRecordRejected(e)
                ? PersistenceException.recordRejected(context + ": " + e.getMessage(), e)
                : PersistenceException.systemic(context + ": " + e.getMessage(), e);
    }
}
