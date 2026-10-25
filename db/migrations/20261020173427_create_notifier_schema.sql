-- migrate:up
CREATE SCHEMA notifier;
CREATE TABLE notifier.deliveries (
    event_id     uuid     PRIMARY KEY,
    payment_id   uuid     NOT NULL,
    delivered_at timestamptz NOT NULL DEFAULT now()
);

-- migrate:down
DROP SCHEMA notifier CASCADE;
