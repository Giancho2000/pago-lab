# payments-api

API de pagos de **PagoLab** (NestJS 12 · TypeScript 6 · ESM). Recibe pagos con idempotencia, los cobra en la pasarela simulada, resuelve los pagos con resultado incierto (`UNKNOWN`) y publica cada cambio de estado en Kafka mediante un *transactional outbox*.

La visión general del proyecto, la arquitectura completa y el guion de demostración están en el [README principal](../../README.md).

---

## Responsabilidades

| Pieza | Dónde | Qué hace |
|---|---|---|
| `PaymentsController` | `src/payments/` | `POST /payments`, `GET /payments/{id}` y `GET /payments` (paginación por cursor) |
| `IdempotencyService` | `src/payments/` | Reclamo atómico de `Idempotency-Key`, *replay* de la respuesta original, `409`/`422` y recuperación de claves huérfanas |
| `PaymentsService` · `PaymentsRepository` | `src/payments/` | Flujo de cobro con timeout y transiciones condicionales (`UPDATE … WHERE status = $from`). Cada transición escribe la auditoría (`payment_events`) y el evento (`outbox`) en la misma transacción |
| `GatewayClient` | `src/payments/` | Cliente HTTP de `gateway-sim` con timeout (`GATEWAY_TIMEOUT_MS`) |
| `UnknownPaymentsResolver` | `src/payments/` | Cada 10 s consulta a la pasarela los pagos `UNKNOWN` y los `PROCESSING` atascados (`FOR UPDATE SKIP LOCKED` + *lease*) |
| Dominio | `src/payments/domain/` | Máquina de estados (`payment-status.ts`) y construcción de eventos (`payment-events.ts`) |
| `OutboxRelay` | `src/messaging/` | Cada 1 s publica el outbox en `payments.events` (`key = paymentId`, `acks=all`) y purga a diario lo publicado hace más de 7 días |
| `KafkaModule` | `src/messaging/` | Productor idempotente de Kafka; se desconecta al apagar la aplicación |
| Health | `src/health/` | `/health/live` y `/health/ready` |
| Comunes | `src/common/` | Errores RFC 9457, helper de transacciones, decorador `X-Merchant-Id` y rate limit por comercio (`rate-limit/`) |

El contrato HTTP es [`docs/openapi.yaml`](docs/openapi.yaml), servido con Swagger UI en `/docs`. El contrato de los eventos es [`specs/fase-2/asyncapi.yaml`](../../specs/fase-2/asyncapi.yaml).

---

## Ejecutar

### Con Docker Compose (recomendado)

Desde la raíz del repositorio:

```bash
docker compose up -d --build payments-api
```

Compose levanta también lo que la API necesita: PostgreSQL, las migraciones, `gateway-sim` y Kafka con sus topics.

### En local (desarrollo)

```bash
# Desde la raíz: dependencias en Docker
docker compose up -d postgres migrate gateway-sim kafka kafka-init

# Desde apps/payments-api
cp .env.example .env      # ajusta los valores (ver tabla)
npm install
npm run start:dev
```

| Variable | Descripción | Valor local |
|---|---|---|
| `PORT` | Puerto HTTP | `3000` |
| `DATABASE_URL` | Conexión a PostgreSQL | `postgres://pagolab:pagolab@localhost:5433/pagolab` |
| `GATEWAY_URL` | URL de la pasarela | `http://localhost:4000` |
| `GATEWAY_TIMEOUT_MS` | Tiempo máximo de espera a la pasarela | `2000` |
| `RESOLVER_ENABLED` | Activa el resolver de pagos `UNKNOWN` | `true` |
| `KAFKA_BROKERS` | Brokers de Kafka | `localhost:29092` (en Compose: `kafka:9092`) |
| `RELAY_ENABLED` | Activa el relay del outbox. Solo **una** instancia debe tenerlo activo, para conservar el orden de los eventos | `true` |
| `RATE_LIMIT_ENABLED` | Activa el límite de creación de pagos por comercio | `true` |
| `RATE_LIMIT_BURST` | Capacidad del *token bucket*: ráfaga máxima de creaciones por comercio | `30` |
| `RATE_LIMIT_PER_SECOND` | Tokens que se recargan por segundo (ritmo sostenido por comercio) | `10` |

Si ejecutas la API en local mientras el contenedor `payments-api` sigue corriendo, deja `RELAY_ENABLED=false` en una de las dos.

---

## Scripts

| Comando | Qué hace | Requiere Docker |
|---|---|---|
| `npm run start:dev` | API en modo *watch* | Sí (dependencias) |
| `npm run build` | Compila a `dist/` | No |
| `npm test` | Pruebas unitarias (proyecto `unit`) | No |
| `npm run test:e2e` | Integración contra PostgreSQL real con Testcontainers (proyecto `e2e`) | Sí |
| `npm run test:cov` | Ambas suites con cobertura y umbral mínimo del 85 % de líneas | Sí |
| `npm run lint` | oxlint sobre `src/` y `test/` | No |
| `npm run format` | Prettier sobre `src/` y `test/` | No |

**Estado:** 125 pruebas en verde · 97,1 % de cobertura de líneas. El reporte HTML queda en `coverage/index.html`.

### Cómo están armadas las pruebas

- **Unitarias** (`src/**/*.spec.ts`): máquina de estados, construcción de eventos, cliente de la pasarela, resolver, relay y transacciones.
- **Integración** (`test/*.e2e-spec.ts`, `test/*.int-spec.ts`): la aplicación completa contra un PostgreSQL efímero.
  - `test/support/global-setup.ts` levanta el contenedor una vez por ejecución y le aplica **todas** las migraciones reales de `db/migrations`.
  - `test/support/fake-gateway.ts` es una pasarela falsa determinista.
  - `test/support/app.ts` reemplaza el productor de Kafka por uno falso, así que las pruebas no necesitan Kafka. El relay se prueba verificando las llamadas a `send`.

---

## Imagen Docker

El `Dockerfile` es *multi-stage*: `deps` (instala dependencias) → `build` (compila y ejecuta `npm prune --omit=dev`) → `runtime` (`node:22-alpine` con usuario `node` y *healthcheck* sobre `/health/live`). Pesa unos 375 MB, frente a 2,21 GB de la versión de una sola etapa.
