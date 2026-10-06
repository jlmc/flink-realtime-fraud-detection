package io.github.jlmc.fraud.bootstrap;

import io.github.jlmc.fraud.domain.risk.RiskThresholds;

import java.io.Serializable;
import java.math.BigDecimal;
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
        String consumerGroup,
        String transactionalIdPrefix,
        int kafkaTransactionTimeoutMs,
        Duration watermarkOutOfOrderness,
        Duration watermarkIdleness,
        Duration allowedLateness,
        Duration dedupRetention,
        RiskThresholds thresholds,
        PostgresConfig postgres) implements Serializable {

    /**
     * PostgreSQL connection and write tuning. The password has a local-development default only (see .env.example);
     * {@link #toString()} never prints it.
     */
    public record PostgresConfig(String url, String user, String password,
                                 int batchSize, long flushIntervalMillis, int maxRetrySeconds) implements Serializable {

        @Override
        public String toString() {
            return "PostgresConfig[url=" + url + ", user=" + user + ", password=***, batchSize=" + batchSize
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
                l.get("kafka.consumer-group", "fraud-risk-job"),
                l.get("kafka.transactional-id-prefix", "fraud-risk"),
                l.getInt("kafka.transaction-timeout-ms", 600_000),
                Duration.ofSeconds(l.getInt("watermark.out-of-orderness-seconds", 30)),
                Duration.ofSeconds(l.getInt("watermark.idleness-seconds", 30)),
                Duration.ofSeconds(l.getInt("watermark.allowed-lateness-seconds", 0)),
                Duration.ofHours(l.getInt("dedup.retention-hours", 24)),
                new RiskThresholds(
                        l.getInt("risk.max-transactions-per-minute", d.maxTransactionsPerMinute()),
                        new BigDecimal(l.get("risk.max-amount-per-ten-minutes", d.maxAmountPerTenMinutes().toPlainString())),
                        new BigDecimal(l.get("risk.anomaly-multiplier", d.anomalyMultiplier().toPlainString())),
                        l.getInt("risk.anomaly-min-history", d.anomalyMinHistory()),
                        d.historyRetention(),
                        d.maxHistoryEntries()),
                new PostgresConfig(
                        l.get("postgres.url", "jdbc:postgresql://postgres:5432/fraud"),
                        l.get("postgres.user", "fraud"),
                        l.get("postgres.password", "fraud"),
                        l.getInt("postgres.batch-size", 500),
                        l.getInt("postgres.flush-interval-ms", 200),
                        l.getInt("postgres.max-retry-seconds", 60)));
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
