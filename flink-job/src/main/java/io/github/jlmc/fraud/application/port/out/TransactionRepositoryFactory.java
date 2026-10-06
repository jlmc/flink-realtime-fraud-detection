package io.github.jlmc.fraud.application.port.out;

import java.io.Serializable;

/** Serializable recipe for a repository: connections are opened on the TaskManager, never serialised. */
@FunctionalInterface
public interface TransactionRepositoryFactory extends Serializable {

    TransactionRepository create();
}
