-- One risk result per transaction. No FK to transactions on purpose: sinks must not depend on write order.
CREATE TABLE risk_scores (
    transaction_id  VARCHAR(100) PRIMARY KEY,
    customer_id     VARCHAR(100) NOT NULL,
    risk_score      SMALLINT     NOT NULL,
    risk_level      VARCHAR(8)   NOT NULL,
    reasons         JSONB        NOT NULL DEFAULT '[]'::jsonb,
    event_time      TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT risk_scores_score_chk CHECK (risk_score BETWEEN 0 AND 100),
    CONSTRAINT risk_scores_level_chk CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH'))
);

CREATE INDEX idx_risk_scores_customer_event_time ON risk_scores (customer_id, event_time);
