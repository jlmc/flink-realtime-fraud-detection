# 0001: PostgreSQL connection pool (HikariCP, shared per TaskManager)

Status: accepted

## Context

The PostgreSQL sink ran one hand-managed connection per sink subtask. That left three gaps: nothing checked that a connection was
still alive between batches, nothing bounded how many connections the job opened as parallelism grew, and reconnect logic was
ours to maintain.

## Decision

Use **HikariCP 7.0.2** and share **one pool per TaskManager JVM** between all sink subtasks running there
(`PooledDataSources`, reference counted: the pool closes when the last subtask closes).

## Why HikariCP

- It is the de facto standard JDBC pool in the Java ecosystem (the default pool of Spring Boot), small, and actively maintained.
- 7.x requires Java 11+, and the project runs on Java 17.
- It validates and evicts broken connections itself, rolls back uncommitted work when a connection is returned, and has
  keep-alive and max-lifetime, which were exactly the gaps.
- 7.0.2 rather than 7.1.0: the newest minor was only just out; we take the line that has been in use for a while.
- Alternatives not chosen: Apache Commons DBCP2 and c3p0 (older designs, slower, more tuning for no gain here).

## Why shared per TaskManager, not one pool per subtask

Sink writers are single-threaded: a subtask uses one connection at a time, so a private pool would only add health management.
A shared pool adds the property that matters for the PLAN's "bounded concurrency" requirement (section 27): PostgreSQL never sees
more than `postgres.pool-size` connections from a TaskManager, however high the parallelism is. Subtasks beyond that wait for a
free connection, which is more backpressure on the Kafka side instead of more load on the database.

Measured on the Compose cluster: parallelism 3 with `--postgres.pool-size 2` gave exactly 2 connections in `pg_stat_activity`,
300 of 300 rows written, and recovery without loss after stopping PostgreSQL for 20 s.

## Settings and why

| Setting | Value | Reason |
|---|---|---|
| `maximumPoolSize` | `postgres.pool-size`, default 4 | Cap per TaskManager. Far below PostgreSQL's default of 100 connections even with several TaskManagers. |
| `minimumIdle` | not set (fixed-size pool) | HikariCP recommends not setting it, so the pool is fixed-size: best performance and no connection created under load. A streaming sink keeps its connections busy anyway. |
| `connectionTimeout` | `postgres.connection-timeout-ms`, default 5000 | With the database down, every attempt waits this long before failing. Kept short so the persister's retry loop (200 ms to 5 s backoff, 60 s budget) stays in control instead of one attempt eating the whole budget. |
| `validationTimeout` | 5 s | Hikari's default; must stay below the connection timeout. |
| `keepaliveTime` | 2 min | Below typical firewall and proxy idle cut-offs, so idle pooled connections are not silently dropped. |
| `maxLifetime` | 30 min | Hikari's default; renews connections regularly and well before any server-side limit we know of. |
| `autoCommit` | false, set explicitly | Every batch is an explicit transaction (all or nothing), which is what makes retrying a batch safe. The repository always commits or rolls back itself; if a caller ever forgets, Hikari rolls back what is left open when the connection returns to the pool (covered by an integration test), and the server-side timeout below is the second net. |
| `transactionIsolation` | `TRANSACTION_READ_COMMITTED` | Explicit instead of "whatever the driver defaults to". |
| `initializationFailTimeout` | -1 | Do not fail at startup when the database is down: the persister retries and the job fails only after its time budget. |
| Data source | `PGSimpleDataSource` handed to Hikari | Going through `jdbcUrl` uses `DriverManager`, which cannot see the driver from Flink's user-code classloader. |
| Driver `socketTimeout` | 30 s | A hung database must not block a writer forever. HikariCP's "Rapid Recovery" page asks for at least 30 s so a dead peer is noticed instead of waiting for the OS TCP timeout (hours). |
| Driver `loginTimeout` | 5 s | Bounds the whole login handshake; `connectTimeout` (5 s) only covers the TCP connect. |
| Session `statement_timeout` | 20 s (via the driver `options`) | The server cancels a runaway statement. Kept below `socketTimeout` so the clean SQLSTATE 57014 reaches us before the client gives up on the socket; it is classified as a system failure and retried. |
| Session `idle_in_transaction_session_timeout` | 60 s | The server kills a session left inside a transaction. With `autoCommit=false` a forgotten transaction would otherwise hold its locks indefinitely. |
| Session `lock_timeout` | 10 s | Never wait unbounded on a lock. |

## Practices checked and not applied

Reviewed against the HikariCP README, its "About Pool Sizing" and "Rapid Recovery" pages and the PostgreSQL JDBC documentation.

- `leakDetectionThreshold`: HikariCP says to leave it off unless leaks are suspected. A connection lives only for one batch and is
  always closed with try-with-resources, so there is nothing to detect; the server-side timeouts cover the rest.
- `connectionTestQuery`: not needed with a JDBC4 driver (Hikari validates with `isValid`).
- Prepared statement caching in the pool: discouraged by HikariCP, the driver does it better.
- Pool size: HikariCP's guidance is a small pool saturated with waiting threads, around `(cores x 2) + spindles` for the database
  server. The default of 4 per TaskManager is deliberately far below that.
- DNS caching: after a database failover that changes its IP, a long JVM DNS cache would delay reconnection. The JVM default (30 s
  positive TTL without a Security Manager) is acceptable here; revisit if the database sits behind a changing address.
- A `TimeZone` session option was tried and dropped: the driver sets the session time zone itself and ignores it. It does not matter
  for correctness because instants are written as UTC `OffsetDateTime`.

Sources: HikariCP README and wiki (github.com/brettwooldridge/HikariCP), PostgreSQL JDBC "Connecting to the database" parameters.

## Consequences and limits

- The pool is static state, so it lives per classloader. Each Flink job, and each restart of it, has its own user-code classloader,
  so pools do not leak between jobs. Writers always release their lease in `close()`.
- With the pool smaller than the parallelism per TaskManager, throughput is bounded by the pool. That is intended; raise
  `postgres.pool-size` if the database can take it.
- The pool lifecycle is unit tested without a database, and `PooledDataSourcesIT` tests it against a real PostgreSQL: connection
  cap with 8 concurrent writers, recovery after the server drops every connection, rollback of a forgotten transaction, the session
  settings, fixed size, and release when the last writer closes.
- Pool metrics (active, idle, waiting connections) are not exposed yet; that belongs to the observability milestone.
