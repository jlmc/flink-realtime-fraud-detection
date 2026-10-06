package io.github.jlmc.fraud.adapter.out.persistence;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Pool lifecycle only: no database is needed (the pool is configured not to fail when the database is unreachable). */
class PooledDataSourcesTest {

    private static PoolConfig config(int size) {
        return new PoolConfig("jdbc:postgresql://127.0.0.1:1/none", "u", "secret", size, 250);
    }

    @Test
    void equalConfigsShareOnePool() {
        try (var a = PooledDataSources.acquire(config(2)); var b = PooledDataSources.acquire(config(2))) {
            assertThat(a.dataSource()).isSameAs(b.dataSource());
            assertThat(((HikariDataSource) a.dataSource()).getMaximumPoolSize()).isEqualTo(2);
        }
    }

    @Test
    void differentConfigsGetDifferentPools() {
        try (var a = PooledDataSources.acquire(config(2)); var b = PooledDataSources.acquire(config(3))) {
            assertThat(a.dataSource()).isNotSameAs(b.dataSource());
        }
    }

    @Test
    void thePoolStaysOpenUntilTheLastLeaseIsClosed() {
        var first = PooledDataSources.acquire(config(2));
        var second = PooledDataSources.acquire(config(2));
        HikariDataSource pool = (HikariDataSource) first.dataSource();

        first.close();
        assertThat(pool.isClosed()).isFalse();

        second.close();
        assertThat(pool.isClosed()).isTrue();
    }

    @Test
    void closingALeaseTwiceDoesNotReleaseSomeoneElsesReference() {
        var first = PooledDataSources.acquire(config(2));
        var second = PooledDataSources.acquire(config(2));
        HikariDataSource pool = (HikariDataSource) first.dataSource();

        first.close();
        first.close(); // must be a no-op

        assertThat(pool.isClosed()).as("second lease is still using it").isFalse();
        second.close();
        assertThat(pool.isClosed()).isTrue();
    }

    @Test
    void acquiringAfterEveryoneLeftCreatesAFreshPool() {
        var first = PooledDataSources.acquire(config(2));
        var firstPool = first.dataSource();
        first.close();

        try (var again = PooledDataSources.acquire(config(2))) {
            assertThat(again.dataSource()).isNotSameAs(firstPool);
            assertThat(((HikariDataSource) again.dataSource()).isClosed()).isFalse();
        }
    }

    @Test
    void noPoolIsLeftBehind() {
        PooledDataSources.acquire(config(5)).close();

        assertThat(PooledDataSources.livePools()).isZero();
    }

    @Test
    void thePasswordIsNeverPrinted() {
        assertThat(config(2).toString()).doesNotContain("secret").contains("password=***");
    }
}
