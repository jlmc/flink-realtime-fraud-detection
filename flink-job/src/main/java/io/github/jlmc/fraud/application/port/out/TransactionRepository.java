package io.github.jlmc.fraud.application.port.out;

import io.github.jlmc.fraud.application.model.PersistableEvent;

import java.util.List;

/**
 * Outbound port for durable business data. Implementations MUST be idempotent: writing the same transaction again
 * (a replay after recovery) must not create a second row nor fail. The whole batch is atomic.
 */
public interface TransactionRepository extends AutoCloseable {

    void saveAll(List<PersistableEvent> batch) throws PersistenceException;

    @Override
    void close();
}
