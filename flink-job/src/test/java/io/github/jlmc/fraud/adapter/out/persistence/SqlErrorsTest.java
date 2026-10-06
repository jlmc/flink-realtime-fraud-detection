package io.github.jlmc.fraud.adapter.out.persistence;

import org.junit.jupiter.api.Test;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class SqlErrorsTest {

    @Test
    void dataAndIntegrityErrorsAreAboutTheRecord() {
        for (String state : new String[] {"22001", "22003", "22P02", "23502", "23514"}) {
            assertThat(SqlErrors.isRecordRejected(new SQLException("x", state))).as(state).isTrue();
            assertThat(SqlErrors.classify("ctx", new SQLException("x", state)).isRecordRejected()).as(state).isTrue();
        }
    }

    @Test
    void everythingElseIsSystemicEspeciallyAMissingTable() {
        for (String state : new String[] {"42P01", "42703", "08006", "08001", "57P01", "53300", "40001", "40P01", "28P01"}) {
            assertThat(SqlErrors.isRecordRejected(new SQLException("x", state))).as(state).isFalse();
        }
        assertThat(SqlErrors.isRecordRejected(new SQLException("no state at all"))).isFalse();
    }

    @Test
    void looksThroughTheChainOfABatchFailure() {
        SQLException batch = new BatchUpdateException("batch", "XX000", new int[0]);
        batch.setNextException(new SQLException("the real cause", "22001"));

        assertThat(SqlErrors.isRecordRejected(batch)).isTrue();
    }
}
