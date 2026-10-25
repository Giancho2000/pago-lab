-- migrate:up
CREATE SCHEMA ledger;
CREATE TABLE ledger.processed_events (
  event_id     uuid        PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ledger.merchant_balances (
  merchant_id    text   NOT NULL,
  currency       text   NOT NULL,
  approved_cents bigint NOT NULL DEFAULT 0 CHECK (approved_cents >= 0),
  approved_count int    NOT NULL DEFAULT 0 CHECK (approved_count >= 0),
  PRIMARY KEY (merchant_id, currency)
);

-- migrate:down
DROP SCHEMA ledger CASCADE;