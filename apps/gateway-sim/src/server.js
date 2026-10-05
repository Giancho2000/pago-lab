import express from 'express';
import { randomUUID } from 'node:crypto';

const app = express();
app.use(express.json());

const cfg = {
  approveRate: Number(process.env.APPROVE_RATE ?? 0.8), // del total procesado
  errorRate: Number(process.env.ERROR_RATE ?? 0.05),    // 503 sin cobrar
  slowRate: Number(process.env.SLOW_RATE ?? 0.15),      // cobra, pero responde tarde
  slowMs: Number(process.env.SLOW_MS ?? 5000),
};

// Guardamos en memoria por si reiniciamos el contenedor, la pasarela "olvida" los cobros
const charges = new Map(); // clave: reference (nuestro id de pago)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

app.post('/charges', async (req, res) => {
  const reference = req.get('idempotency-key');
  if (!reference) return res.status(400).json({ error: 'idempotency-key requerido' });

  // La pasarela también es idempotente: misma referencia, mismo resultado
  const existing = charges.get(reference);
  if (existing) return res.status(existing.status === 'DECLINED' ? 402 : 201).json(existing);

  if (Math.random() < cfg.errorRate) {
    return res.status(503).json({ error: 'servicio no disponible' }); // NO se registró el cobro
  }

  const status = Math.random() < cfg.approveRate ? 'APPROVED' : 'DECLINED';
  const charge = {
    id: `ch_${randomUUID()}`,
    reference,
    amountCents: req.body.amountCents,
    currency: req.body.currency,
    status,
    reason: status === 'DECLINED' ? 'fondos insuficientes' : undefined,
  };
  charges.set(reference, charge); // el cobro YA ocurrió...

  if (Math.random() < cfg.slowRate) await sleep(cfg.slowMs); // ...pero la respuesta llega tarde

  return res.status(status === 'DECLINED' ? 402 : 201).json(charge);
});

app.get('/charges/:reference', (req, res) => {
  const charge = charges.get(req.params.reference);
  return charge ? res.json(charge) : res.status(404).json({ error: 'no encontrado' });
});

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

const port = Number(process.env.PORT ?? 4000);
app.listen(port, () => console.log(`gateway-sim escuchando en :${port}`, cfg));