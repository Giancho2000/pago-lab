-- migrate:up
-- Token bucket por comercio para limitar la creación de pagos.
-- Una fila por comercio: tokens disponibles y el instante de la última recarga.
-- Vive en PostgreSQL para que el límite sea el mismo con cualquier número de réplicas de la API.
CREATE TABLE rate_limit_buckets (
  merchant_id  text             PRIMARY KEY,
  tokens       double precision NOT NULL CHECK (tokens >= 0),
  refilled_at  timestamptz      NOT NULL
);

-- migrate:down
DROP TABLE rate_limit_buckets;
