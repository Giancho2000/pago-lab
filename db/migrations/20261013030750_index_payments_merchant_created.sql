-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY idx_payments_merchant_created
  ON payments (merchant_id, created_at DESC, id DESC);

-- migrate:down transaction:false
DROP INDEX CONCURRENTLY idx_payments_merchant_created;