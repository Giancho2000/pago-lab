import express from 'express';
import { createHmac, timingSafeEqual } from 'node:crypto';

const SECRET = process.env.WEBHOOK_SECRET ?? 'whsec_local';
const TOLERANCE_S = 300;                            // 5 minutos
const FAIL_RATE = Number(process.env.FAIL_RATE ?? 0.2);

const seen = new Set();

const app = express();

app.post('/webhooks/pagolab', express.raw({ type: 'application/json' }), (req, res) => {
  const header = req.get('pagolab-signature') ?? '';
  const m = /^t=(\d+),v1=([0-9a-f]{64})$/.exec(header);

  if (!m) return res.status(400).json({ error: 'cabecera de firma ausente o mal formada' });
  const t = Number(m[1]);
  const v1 = m[2];

  if (Math.abs(Date.now() / 1000 - t) > TOLERANCE_S) {
    return res.status(400).json({ error: 'timestamp fuera de tolerancia (posible replay)' });
  }

  const expected = createHmac('sha256', SECRET).update(`${t}.${req.body}`).digest('hex');
  const a = Buffer.from(expected);
  const b = Buffer.from(v1);
  if (a.length !== b.length || !timingSafeEqual(a, b)) {   // comparación en tiempo constante
    return res.status(401).json({ error: 'firma inválida' });
  }

  let event;
  // Validamos que sea un JSON valido
  try {
    event = JSON.parse(req.body.toString());

    if (typeof event?.eventId !== 'string' || typeof event?.data?.id !== 'string') {
      return res.status(400).json({ error: 'evento sin eventId o data.id' });
    }
  } catch (error) {
    return res.status(400).json({ error: 'cuerpo no es JSON válido' }); // Cae en 400 para que no haga reintentos con un JSON mal formado
  }

  if (Math.random() < FAIL_RATE) return res.status(500).json({ error: 'fallo simulado' }); // Al ser 500 se hacen reintentos.

  if (seen.has(event.eventId)) {
    console.log(`DUPLICADO ignorado ${event.eventId}`);
    return res.status(200).json({ duplicate: true });
  }
  seen.add(event.eventId);
  console.log(`OK ${event.type} pago=${event.data.id}`);
  return res.status(200).json({ received: true });
});

app.listen(Number(process.env.PORT ?? 4100), () => console.log('merchant-sim en :4100'));