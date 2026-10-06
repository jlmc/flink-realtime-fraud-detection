-- At most one alert per transaction (alerts are raised for risky results; policy lives in the application).
CREATE TABLE fraud_alerts (
    transaction_id  VARCHAR(100) PRIMARY KEY,
    customer_id     VARCHAR(100) NOT NULL,
    risk_score      SMALLINT     NOT NULL,
    risk_level      VARCHAR(8)   NOT NULL,
    reasons         JSONB        NOT NULL DEFAULT '[]'::jsonb,
    event_time      TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_fraud_alerts_customer ON fraud_alerts (customer_id);
