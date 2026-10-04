-- migrate:up
CREATE TABLE payments (
  id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  merchant_id   text        NOT NULL,
  amount_cents  bigint      NOT NULL CHECK (amount_cents > 0),
  currency      char(3)     NOT NULL,
  description   text,
  status        text        NOT NULL DEFAULT 'PENDING'
                CHECK (status IN ('PENDING','PROCESSING','APPROVED','DECLINED','UNKNOWN','FAILED')),
  gateway_ref   text,
  attempts      int         NOT NULL DEFAULT 0,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE payment_events (
  id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payment_id   uuid        NOT NULL REFERENCES payments(id),
  from_status  text,
  to_status    text        NOT NULL,
  reason       text,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_payment_events_payment ON payment_events (payment_id, created_at);

-- migrate:down
DROP TABLE payment_events;
DROP TABLE payments;
