-- migrate:up
ALTER TABLE payments ADD COLUMN idempotency_key text;

CREATE UNIQUE INDEX ux_payments_merchant_idem
  ON payments (merchant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

ALTER TABLE idempotency_keys
  ADD COLUMN locked_until timestamptz NOT NULL DEFAULT now() + interval '60 seconds';

-- migrate:down
ALTER TABLE idempotency_keys DROP COLUMN locked_until;
DROP INDEX ux_payments_merchant_idem;
ALTER TABLE payments DROP COLUMN idempotency_key;