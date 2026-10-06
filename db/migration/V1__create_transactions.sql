-- Business record of every transaction that passed validation (idempotent key: transaction_id).
CREATE TABLE transactions (
    transaction_id  VARCHAR(100)   PRIMARY KEY,
    customer_id     VARCHAR(100)   NOT NULL,
    merchant_id     VARCHAR(100)   NOT NULL,
    amount          NUMERIC(19, 4) NOT NULL,
    currency        CHAR(3)        NOT NULL,
    country         VARCHAR(2),
    event_time      TIMESTAMPTZ    NOT NULL,
    status          VARCHAR(16)    NOT NULL DEFAULT 'PROCESSED',  -- PROCESSED | LATE
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT transactions_status_chk CHECK (status IN ('PROCESSED', 'LATE'))
);

CREATE INDEX idx_transactions_customer_event_time ON transactions (customer_id, event_time);
