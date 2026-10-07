package io.github.jlmc.fraud.bootstrap;

import io.github.jlmc.fraud.domain.risk.RiskThresholds;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Job configuration. Resolution order for each key: program argument ({@code --kafka.bootstrap-servers x}), then
 * environment variable ({@code KAFKA_BOOTSTRAP_SERVERS}), then the default. No credentials have defaults except the
 * local-development ones documented in .env.example.
 */
public record JobConfig(
        String kafkaBootstrapServers,
        String transactionsTopic,
        String riskTopic,
        String invalidTopic,
        String alertsTopic,
        String consumerGroup,
        String transactionalIdPrefix,
        String alertsTransactionalIdPrefix,
        int kafkaTransactionTimeoutMs,
        Duration watermarkOutOfOrderness,
        Duration watermarkIdleness,
        Duration allowedLateness,
        Duration dedupRetention,
        RiskThresholds thresholds,
        Map<String, String> riskSettings,
        PostgresConfig postgres) implements Serializable {

    /**
     * PostgreSQL connection and write tuning. The password has a local-development default only (see .env.example);
     * {@link #toString()} never prints it.
     */
    public record PostgresConfig(String url, String user, String password,
                                 int poolSize, long connectionTimeoutMillis,
                                 int batchSize, long flushIntervalMillis, int maxRetrySeconds) implements Serializable {

        @Override
        public String toString() {
            return "PostgresConfig[url=" + url + ", user=" + user + ", password=***, poolSize=" + poolSize
                    + ", connectionTimeoutMillis=" + connectionTimeoutMillis + ", batchSize=" + batchSize
                    + ", flushIntervalMillis=" + flushIntervalMillis + ", maxRetrySeconds=" + maxRetrySeconds + "]";
        }
    }

    public static JobConfig from(Map<String, String> args, Map<String, String> env) {
        Lookup l = new Lookup(args, env);
        RiskThresholds d = RiskThresholds.defaults();
        return new JobConfig(
                l.get("kafka.bootstrap-servers", "kafka:19092"),
                l.get("topic.transactions", "transaction.events"),
                l.get("topic.risk", "transaction.risk.events"),
                l.get("topic.invalid", "transaction.invalid.events"),
                l.get("topic.alerts", "fraud.high-risk.alerts"),
                l.get("kafka.consumer-group", "fraud-risk-job"),
                l.get("kafka.transactional-id-prefix", "fraud-risk"),
                // must differ from the risk sink's prefix: transactional ids are owned per sink
                l.get("kafka.alerts-transactional-id-prefix", "fraud-alerts"),
                l.getInt("kafka.transaction-timeout-ms", 600_000),
                Duration.ofSeconds(l.getInt("watermark.out-of-orderness-seconds", 30)),
                Duration.ofSeconds(l.getInt("watermark.idleness-seconds", 30)),
                Duration.ofSeconds(l.getInt("watermark.allowed-lateness-seconds", 0)),
                Duration.ofHours(l.getInt("dedup.retention-hours", 24)),
                d,
                riskSettings(args, env),
                new PostgresConfig(
                        l.get("postgres.url", "jdbc:postgresql://postgres:5432/fraud"),
                        l.get("postgres.user", "fraud"),
                        l.get("postgres.password", "fraud"),
                        l.getInt("postgres.pool-size", 4),
                        l.getInt("postgres.connection-timeout-ms", 5000),
                        l.getInt("postgres.batch-size", 500),
                        l.getInt("postgres.flush-interval-ms", 200),
                        l.getInt("postgres.max-retry-seconds", 60)));
    }

    /**
     * Every {@code risk.*} setting, handed to the risk rule plugins. A program argument ({@code --risk.x-y 3}) wins over
     * an environment variable ({@code RISK_X_Y=3}). Kept as a {@link HashMap}: the config is serialised with the job.
     */
    static Map<String, String> riskSettings(Map<String, String> args, Map<String, String> env) {
        HashMap<String, String> settings = new HashMap<>();
        env.forEach((name, value) -> {
            if (name.startsWith("RISK_")) {
                settings.put("risk." + name.substring("RISK_".length()).toLowerCase(Locale.ROOT).replace('_', '-'), value);
            }
        });
        args.forEach((key, value) -> {
            if (key.startsWith("risk.")) {
                settings.put(key, value);
            }
        });
        return settings;
    }

    /** Parses {@code --key value} pairs. A key without a value is an error: silently ignoring it hides typos. */
    public static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parsed = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("Expected '--key value' pairs, got: " + String.join(" ", args));
            }
            parsed.put(args[i].substring(2), args[i + 1]);
        }
        return parsed;
    }

    private record Lookup(Map<String, String> args, Map<String, String> env) {

        String get(String key, String fallback) {
            String fromArgs = args.get(key);
            if (fromArgs != null) {
                return fromArgs;
            }
            String fromEnv = env.get(key.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_'));
            return fromEnv != null ? fromEnv : fallback;
        }

        int getInt(String key, int fallback) {
            return Integer.parseInt(get(key, String.valueOf(fallback)));
        }
    }
}
