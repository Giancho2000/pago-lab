INSERT INTO payments (merchant_id, amount_cents, currency, status, created_at, updated_at)
SELECT 'merchant_' || (random() * 50)::int,
       (random() * 1000000)::bigint + 100,
       'COP',
       (ARRAY['APPROVED','DECLINED','FAILED'])[1 + (random() * 2)::int],
       ts, ts
FROM (SELECT now() - random() * interval '365 days' AS ts
        FROM generate_series(1, 1000000)) s;

ANALYZE payments;