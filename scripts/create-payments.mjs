// Crea N pagos contra la API local. Funciona igual en Git Bash, PowerShell o cmd.
// Uso: node scripts/create-payments.mjs [cantidad=15] [comercio=merchant_demo]
const count = Number(process.argv[2] ?? 15);
const merchant = process.argv[3] ?? 'merchant_demo';
const api = process.env.API_URL ?? 'http://localhost:3000';

const results = await Promise.all(
  Array.from({ length: count }, async (_, i) => {
    const res = await fetch(`${api}/payments`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Merchant-Id': merchant,
        'Idempotency-Key': crypto.randomUUID(),
      },
      body: JSON.stringify({ amountCents: (i + 1) * 1000, currency: 'COP' }),
    });
    const body = await res.json();
    return { http: res.status, status: body.status ?? body.title, id: body.id };
  }),
);

console.table(results);
const byStatus = Object.groupBy(results, (r) => r.status);
console.log(Object.fromEntries(Object.entries(byStatus).map(([k, v]) => [k, v.length])));
