import {
  createServer,
  IncomingMessage,
  Server,
  ServerResponse,
} from 'node:http';
import { AddressInfo } from 'node:net';

/**
 * Comportamiento del próximo POST /charges:
 * - approve / decline: cobra y responde 201 / 402
 * - error: 503 sin registrar el cobro
 * - reject: 400 (la pasarela rechaza la petición)
 * - delay: cobra y responde 201 tras `delayMs` (por debajo del timeout)
 * - hang: cobra pero responde tras `hangMs` (por encima del timeout)
 */
export type ChargeBehavior =
  'approve' | 'decline' | 'error' | 'reject' | 'delay' | 'hang';

interface Charge {
  id: string;
  status: 'APPROVED' | 'DECLINED';
  reason?: string;
}

// Pasarela determinista para las pruebas, con el mismo contrato que apps/gateway-sim
export class FakeGateway {
  readonly charges = new Map<string, Charge>();
  readonly chargeCalls: string[] = [];
  delayMs = 250;
  hangMs = 1_500;
  lookupStatus: number | null = null; // fuerza el status de GET /charges/:ref
  url = '';

  private queue: ChargeBehavior[] = [];
  private server?: Server;
  private seq = 0;

  next(...behaviors: ChargeBehavior[]) {
    this.queue.push(...behaviors);
  }

  record(reference: string, status: Charge['status']) {
    const charge: Charge = { id: `ch_${++this.seq}`, status };
    this.charges.set(reference, charge);
    return charge;
  }

  reset() {
    this.charges.clear();
    this.chargeCalls.length = 0;
    this.queue = [];
    this.lookupStatus = null;
  }

  async start() {
    this.server = createServer((req, res) => void this.handle(req, res));
    await new Promise<void>((r) => this.server!.listen(0, '127.0.0.1', r));
    this.url = `http://127.0.0.1:${(this.server.address() as AddressInfo).port}`;
  }

  async stop() {
    this.server?.closeAllConnections();
    await new Promise((r) => this.server?.close(r));
  }

  private async handle(req: IncomingMessage, res: ServerResponse) {
    for await (const _ of req); // consumir el cuerpo
    const send = (status: number, body: unknown) => {
      if (res.writableEnded || res.destroyed) return;
      res
        .writeHead(status, { 'content-type': 'application/json' })
        .end(JSON.stringify(body));
    };

    const lookup = req.method === 'GET' && req.url?.match(/^\/charges\/(.+)$/);
    if (lookup) {
      if (this.lookupStatus) return send(this.lookupStatus, {});
      const charge = this.charges.get(lookup[1]);
      return charge ? send(200, charge) : send(404, { error: 'no encontrado' });
    }

    if (req.method !== 'POST' || req.url !== '/charges') return send(404, {});

    const reference = req.headers['idempotency-key'] as string;
    this.chargeCalls.push(reference);

    const existing = this.charges.get(reference);
    if (existing)
      return send(existing.status === 'DECLINED' ? 402 : 201, existing);

    const behavior = this.queue.shift() ?? 'approve';
    if (behavior === 'error')
      return send(503, { error: 'servicio no disponible' });
    if (behavior === 'reject') return send(400, { error: 'petición inválida' });

    const charge = this.record(
      reference,
      behavior === 'decline' ? 'DECLINED' : 'APPROVED',
    );
    if (behavior === 'decline') charge.reason = 'fondos insuficientes';
    const wait =
      behavior === 'delay'
        ? this.delayMs
        : behavior === 'hang'
          ? this.hangMs
          : 0;
    if (wait) await new Promise((r) => setTimeout(r, wait));
    send(charge.status === 'DECLINED' ? 402 : 201, charge);
  }
}
