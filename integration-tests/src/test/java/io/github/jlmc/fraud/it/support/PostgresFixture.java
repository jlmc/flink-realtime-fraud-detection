package io.github.jlmc.fraud.it.support;

import org.flywaydb.core.Flyway;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One PostgreSQL container per JVM, started on first use (the container is reaped by Testcontainers at the end).
 * Every test asks for its own database, migrated with the REAL scripts in db/migration, so destructive tests (dropped
 * tables, killed connections) cannot disturb each other and the schema itself is under test.
 */
public final class PostgresFixture {

    private static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer("postgres:17.11")
            .withDatabaseName("postgres")
            .withUsername("fraud")
            .withPassword("fraud");

    private static final AtomicInteger COUNTER = new AtomicInteger();

    static {
        CONTAINER.start();
    }

    private PostgresFixture() {
    }

    public record Database(String name, String jdbcUrl, String user, String password) {

        public Connection connect() throws SQLException {
            return DriverManager.getConnection(jdbcUrl, user, password);
        }

        public long count(String table) {
            try (Connection c = connect(); Statement s = c.createStatement(); var rs = s.executeQuery("select count(*) from " + table)) {
                rs.next();
                return rs.getLong(1);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        public void execute(String sql) {
            try (Connection c = connect(); Statement s = c.createStatement()) {
                s.execute(sql);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** A brand-new database with the schema applied. */
    public static Database newDatabase() {
        String name = "it_" + COUNTER.incrementAndGet() + "_" + System.nanoTime();
        try (Connection admin = DriverManager.getConnection(CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
             Statement s = admin.createStatement()) {
            s.execute("create database " + name);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        String url = "jdbc:postgresql://" + CONTAINER.getHost() + ":" + CONTAINER.getMappedPort(5432) + "/" + name;
        Flyway.configure()
                .dataSource(url, CONTAINER.getUsername(), CONTAINER.getPassword())
                .locations("filesystem:" + System.getProperty("migration.dir"))
                .load()
                .migrate();
        return new Database(name, url, CONTAINER.getUsername(), CONTAINER.getPassword());
    }

    /** Freezes the whole server (like a network partition or a stalled disk): connections hang, nothing is refused. */
    public static void pause() {
        CONTAINER.getDockerClient().pauseContainerCmd(CONTAINER.getContainerId()).exec();
    }

    public static void unpause() {
        CONTAINER.getDockerClient().unpauseContainerCmd(CONTAINER.getContainerId()).exec();
    }

    /** Opens a connection to the maintenance database, for server-wide operations such as terminating backends. */
    public static Connection admin() throws SQLException {
        return DriverManager.getConnection(CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
    }
}
