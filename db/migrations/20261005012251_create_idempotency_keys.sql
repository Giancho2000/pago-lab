-- migrate:up
CREATE TABLE idempotency_keys (
  merchant_id    text        NOT NULL,
  key            text        NOT NULL,
  request_hash   text        NOT NULL,
  status         text        NOT NULL DEFAULT 'IN_PROGRESS'
                 CHECK (status IN ('IN_PROGRESS','COMPLETED')),
  response_code  int,
  response_body  jsonb,
  created_at     timestamptz NOT NULL DEFAULT now(),
  expires_at     timestamptz NOT NULL DEFAULT now() + interval '24 hours',
  PRIMARY KEY (merchant_id, key)
);

-- migrate:down
DROP TABLE idempotency_keys;