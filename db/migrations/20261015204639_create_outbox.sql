-- migrate:up
CREATE TABLE outbox (
  id            uuid        PRIMARY KEY,
  aggregate_id  uuid        NOT NULL,          -- id del pago: será la clave en Kafka
  event_type    text        NOT NULL,
  payload       jsonb       NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz
);
CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

-- migrate:down
DROP TABLE outbox;