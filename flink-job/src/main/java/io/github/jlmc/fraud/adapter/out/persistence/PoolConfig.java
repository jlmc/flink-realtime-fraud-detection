package io.github.jlmc.fraud.adapter.out.persistence;

import java.io.Serializable;

/**
 * Identity and sizing of a connection pool. Two writers with equal configs share one pool.
 *
 * @param maxPoolSize          hard cap on simultaneous connections for the whole TaskManager, whatever the parallelism
 * @param connectionTimeoutMs  how long a writer waits for a connection before the attempt fails. Keep it short: with
 *                             the database down every attempt waits this long, and the persister's own backoff and
 *                             time budget sit on top of it.
 */
public record PoolConfig(String url, String user, String password, int maxPoolSize, long connectionTimeoutMs) implements Serializable {

    @Override
    public String toString() {
        return "PoolConfig[url=" + url + ", user=" + user + ", password=***, maxPoolSize=" + maxPoolSize
                + ", connectionTimeoutMs=" + connectionTimeoutMs + "]";
    }
}
