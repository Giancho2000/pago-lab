-- migrate:up
ALTER TABLE payments ADD COLUMN next_check_at timestamptz;

-- Índice parcial: solo indexa los pagos pendientes de resolver (pocos), no los millones finalizados
CREATE INDEX idx_payments_to_resolve
  ON payments (updated_at)
  WHERE status IN ('UNKNOWN', 'PROCESSING');

-- migrate:down
DROP INDEX idx_payments_to_resolve;
ALTER TABLE payments DROP COLUMN next_check_at;