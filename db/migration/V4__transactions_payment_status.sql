-- Outcome of the payment attempt itself (APPROVED | DECLINED), distinct from the pipeline status above.
-- Existing rows and producers that do not send the field are approved attempts.
ALTER TABLE transactions
    ADD COLUMN payment_status VARCHAR(8) NOT NULL DEFAULT 'APPROVED',
    ADD CONSTRAINT transactions_payment_status_chk CHECK (payment_status IN ('APPROVED', 'DECLINED'));
