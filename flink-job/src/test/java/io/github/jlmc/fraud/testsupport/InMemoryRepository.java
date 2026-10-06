package io.github.jlmc.fraud.testsupport;

import io.github.jlmc.fraud.application.model.PersistableEvent;
import io.github.jlmc.fraud.application.port.out.PersistenceException;
import io.github.jlmc.fraud.application.port.out.TransactionRepository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Idempotent in-memory repository (keyed by transactionId, first write wins) with scriptable failures. */
public class InMemoryRepository implements TransactionRepository {

    public final Map<String, PersistableEvent> stored = new LinkedHashMap<>();
    public final List<Integer> batchSizes = new ArrayList<>();
    public int calls;

    /** Called before each save; return null to proceed or an exception to throw. */
    public Function<List<PersistableEvent>, PersistenceException> failure = batch -> null;

    @Override
    public void saveAll(List<PersistableEvent> batch) {
        calls++;
        PersistenceException toThrow = failure.apply(batch);
        if (toThrow != null) {
            throw toThrow;
        }
        batchSizes.add(batch.size());
        batch.forEach(e -> stored.putIfAbsent(e.transaction().transactionId(), e));
    }

    @Override
    public void close() {
    }
}
