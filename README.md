# PagoLab

**PagoLab** es un simulador de procesamiento de pagos. Lo construyo como laboratorio para aplicar prácticas de backend de nivel productivo en un dominio donde los errores cuestan dinero: cobros duplicados, pasarelas que no responden y estados que nadie sabe resolver.

El proyecto sigue un enfoque **Spec-Driven Development (SDD)**: primero se escriben los requisitos (EARS) y el contrato de la API (OpenAPI, *design-first*), y después el código. Cada ticket es una rama y un *pull request* con su propio historial.


---

## Contenido

1. [Qué demuestra este proyecto](#qué-demuestra-este-proyecto)
2. [Arquitectura](#arquitectura)
3. [Stack tecnológico](#stack-tecnológico)
4. [Estructura del repositorio](#estructura-del-repositorio)
5. [Puesta en marcha](#puesta-en-marcha)
6. [Uso de la API](#uso-de-la-api)
7. [Pruebas y cobertura](#pruebas-y-cobertura)
8. [Resultados medidos](#resultados-medidos)
9. [Decisiones y limitaciones](#decisiones-y-limitaciones)
10. [Guion de demostración (5 minutos)](#guion-de-demostración-5-minutos)
11. [Historial de construcción](#historial-de-construcción)
12. [Hoja de ruta](#hoja-de-ruta)
13. [Documentación](#documentación)

---

## Qué demuestra este proyecto

| Tema | Cómo se resuelve en PagoLab |
|---|---|
| **Idempotencia** | Cabecera `Idempotency-Key` con reclamo atómico (`INSERT … ON CONFLICT DO NOTHING`). Si se repite la petición, devuelve la respuesta original con `Idempotent-Replayed: true`. Responde `422` si el cuerpo es distinto y `409` si la petición original sigue en curso. |
| **Prevención de doble cobro** | El id del pago viaja como clave de idempotencia hacia la pasarela, y cada cambio de estado es un `UPDATE … WHERE status = $from`: si otro proceso se adelantó, la operación no tiene efecto. |
| **Timeouts honestos** | Si la pasarela no responde dentro de `GATEWAY_TIMEOUT_MS`, el pago queda en `UNKNOWN` (`202`), **nunca** en `FAILED`: no hay certeza de que el cobro no ocurrió. |
| **Resolución concurrente segura** | Un proceso periódico consulta a la pasarela los pagos `UNKNOWN` y los `PROCESSING` atascados. Usa `FOR UPDATE SKIP LOCKED` y un *lease* (`next_check_at`), así que cada pago lo procesa una sola réplica a la vez. |
| **Máquina de estados auditable** | Solo se permiten transiciones válidas, y cada una queda registrada en `payment_events` dentro de la misma transacción. |
| **Paginación eficiente** | Paginación por cursor (*keyset*) sobre `(created_at, id)` con un índice compuesto. El tiempo de respuesta no se degrada al avanzar páginas. |
| **Operación** | Endpoints `/health/live` y `/health/ready`, apagado ordenado ante `SIGTERM` (deja de estar listo, termina las peticiones en curso y cierra el pool) y errores en formato **RFC 9457** (`application/problem+json`). |
| **Pruebas de verdad** | Pruebas de integración contra un PostgreSQL real con **Testcontainers** y una pasarela falsa determinista. Incluyen escenarios de concurrencia. |
| **Contenedores** | Dockerfile *multi-stage* con un usuario sin privilegios y *healthcheck*. Todo el stack se levanta con un solo comando de Docker Compose. |

---

## Arquitectura

```mermaid
flowchart LR
  C[Comercio / cliente HTTP] -->|"POST /payments<br/>X-Merchant-Id + Idempotency-Key"| API

  subgraph API[payments-api · NestJS]
    CTRL[PaymentsController] --> IDEM[IdempotencyService]
    IDEM --> SVC[PaymentsService]
    SVC --> REPO[PaymentsRepository]
    RES[UnknownPaymentsResolver<br/>cada 10 s] --> REPO
  end

  SVC -->|"POST /charges<br/>timeout"| GW[gateway-sim]
  RES -->|"GET /charges/:id"| GW
  REPO --> PG[(PostgreSQL 17<br/>payments · payment_events<br/>idempotency_keys)]
  MIG[dbmate] -->|migraciones| PG
```

### Máquina de estados del pago

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> PROCESSING
    PROCESSING --> APPROVED
    PROCESSING --> DECLINED
    PROCESSING --> FAILED
    PROCESSING --> UNKNOWN : timeout / 5xx
    UNKNOWN --> APPROVED : resolver
    UNKNOWN --> DECLINED : resolver
    UNKNOWN --> FAILED : no registrado tras 3 intentos
```

### Servicios

| Servicio | Descripción | Puerto local |
|---|---|---|
| `postgres` | PostgreSQL 17 con volumen persistente y *healthcheck* | `5433` |
| `migrate` | dbmate: aplica las migraciones de `db/migrations` y termina | — |
| `gateway-sim` | Pasarela simulada (Express) con aprobaciones, rechazos, errores `503` y respuestas lentas configurables | `4000` |
| `payments-api` | API de pagos (NestJS) | `3000` |

---

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Lenguaje y runtime | TypeScript 6 · Node.js 22 (ESM) |
| Framework | NestJS 12 (`@nestjs/schedule`, `@nestjs/swagger`, `class-validator`) |
| Base de datos | PostgreSQL 17 · driver `pg` (SQL explícito, sin ORM) |
| Migraciones | dbmate 2 |
| Pruebas | Vitest 4 · Supertest · Testcontainers · cobertura con V8 |
| Calidad | oxlint · Prettier |
| Contenedores | Docker (multi-stage) · Docker Compose |
| Contrato | OpenAPI 3 (*design-first*), servido con Swagger UI |

---

## Estructura del repositorio

```text
pagos-lab/
├── apps/
│   ├── payments-api/            # API de pagos (NestJS)
│   │   ├── docs/openapi.yaml    # Contrato de la API (fuente de verdad)
│   │   ├── src/
│   │   │   ├── common/          # Filtro RFC 9457, transacciones, X-Merchant-Id
│   │   │   ├── database/        # Pool de PostgreSQL y cierre ordenado
│   │   │   ├── health/          # Liveness y readiness
│   │   │   └── payments/        # Dominio, controlador, servicio, repositorio,
│   │   │                        # idempotencia, cliente de pasarela y resolver
│   │   ├── test/                # Pruebas e2e (Testcontainers + pasarela falsa)
│   │   └── Dockerfile
│   └── gateway-sim/             # Pasarela de pagos simulada
├── db/
│   ├── migrations/              # Migraciones de dbmate
│   └── seed/seed_1m.sql         # 1.000.000 de pagos para el laboratorio de EXPLAIN
├── specs/fase-1/                # Requisitos (EARS) y diseño
└── docker-compose.yml
```

---

## Puesta en marcha

### Requisitos previos

- Docker Desktop (o Docker Engine con Compose v2)
- Node.js 22 y npm, solo para ejecutar las pruebas o la API fuera de Docker
- Git Bash, WSL o una terminal POSIX para los comandos de este documento

### Levantar el stack completo

```bash
git clone https://github.com/Giancho2000/pago-lab.git
cd pago-lab
docker compose up -d --build
```

Compose arranca PostgreSQL, espera su *healthcheck*, aplica las migraciones y luego inicia la pasarela y la API.

### Verificar

```bash
docker compose ps                         # postgres y payments-api en estado "healthy"
curl localhost:3000/health/live           # {"status":"ok"}
curl localhost:3000/health/ready          # {"status":"ready"}
```

La documentación interactiva de la API está en **http://localhost:3000/docs**.

### Ejecutar la API fuera de Docker (desarrollo)

```bash
docker compose up -d postgres migrate gateway-sim
cd apps/payments-api
cp .env.example .env                      # ajusta los valores (ver tabla)
npm install
npm run start:dev
```

| Variable | Descripción | Valor local |
|---|---|---|
| `PORT` | Puerto HTTP de la API | `3000` |
| `DATABASE_URL` | Cadena de conexión a PostgreSQL | `postgres://pagolab:pagolab@localhost:5433/pagolab` |
| `GATEWAY_URL` | URL de la pasarela | `http://localhost:4000` |
| `GATEWAY_TIMEOUT_MS` | Tiempo máximo de espera a la pasarela | `2000` |
| `RESOLVER_ENABLED` | Activa el resolver de pagos `UNKNOWN` | `true` |

### Detener

```bash
docker compose down        # conserva los datos
docker compose down -v     # elimina también el volumen de PostgreSQL
```

---

## Uso de la API

Todas las peticiones requieren la cabecera `X-Merchant-Id`, que identifica al comercio (es un mecanismo temporal; ver [limitaciones](#decisiones-y-limitaciones)).

| Método | Ruta | Descripción | Respuestas |
|---|---|---|---|
| `POST` | `/payments` | Crea un pago y lo cobra en la pasarela. Requiere `Idempotency-Key` (UUID). | `201` estado final · `202` `UNKNOWN` · `400` · `401` · `409` · `422` |
| `GET` | `/payments/{id}` | Consulta un pago del comercio | `200` · `400` · `401` · `404` |
| `GET` | `/payments?limit=&cursor=` | Lista los pagos del comercio, del más reciente al más antiguo | `200` · `400` · `401` |
| `GET` | `/health/live` | El proceso responde | `200` |
| `GET` | `/health/ready` | Lista para recibir tráfico (BD disponible y sin apagado en curso) | `200` · `503` |

### Ejemplo

```bash
KEY=$(node -e "console.log(crypto.randomUUID())")

curl -i -X POST localhost:3000/payments \
  -H "Content-Type: application/json" \
  -H "X-Merchant-Id: merchant_demo" \
  -H "Idempotency-Key: $KEY" \
  -d '{"amountCents":15000,"currency":"COP","description":"Pedido 1001"}'
```

```http
HTTP/1.1 201 Created
Location: /payments/7c1e…

{
  "id": "7c1e…",
  "merchantId": "merchant_demo",
  "amountCents": 15000,
  "currency": "COP",
  "description": "Pedido 1001",
  "status": "APPROVED",
  "gatewayRef": "ch_…",
  "attempts": 0,
  "createdAt": "…",
  "updatedAt": "…"
}
```

El listado devuelve `{ "items": [...], "nextCursor": "…" }`. Para pedir la página siguiente se envía ese valor en `cursor`; `nextCursor` es `null` cuando no hay más páginas.

Los errores siguen el formato RFC 9457:

```json
{
  "type": "about:blank",
  "title": "UNPROCESSABLE_ENTITY",
  "status": 422,
  "detail": "La Idempotency-Key ya se usó con otro cuerpo",
  "instance": "/payments"
}
```

---

## Pruebas y cobertura

Desde `apps/payments-api`:

| Comando | Qué ejecuta | Requiere Docker |
|---|---|---|
| `npm test` | Pruebas unitarias | No |
| `npm run test:e2e` | Pruebas de integración contra PostgreSQL real (Testcontainers) | Sí |
| `npm run test:cov` | Ambas suites con reporte de cobertura y umbral mínimo del 85 % de líneas | Sí |

El reporte HTML queda en `apps/payments-api/coverage/index.html`.

**Estado actual:** 77 pruebas en verde · **98,4 %** de cobertura de líneas.

### Estrategia

- **Unitarias:** máquina de estados, cliente de la pasarela (respuestas `201`/`402`/`4xx`/`5xx` y timeout), ciclo del resolver y manejo de transacciones.
- **Integración (e2e):** la aplicación completa contra un PostgreSQL efímero, con las migraciones reales y una pasarela falsa determinista. Las pruebas están organizadas por requisito (R1–R9) e incluyen:
  - repetición idempotente, conflictos `409`/`422` y liberación de la clave ante fallos previos al cobro;
  - timeout que deja el pago en `UNKNOWN` aunque la pasarela sí haya cobrado;
  - dos resolvers concurrentes que nunca procesan el mismo pago;
  - paginación completa siguiendo `nextCursor`, con empates de `created_at` y pagos creados en el mismo milisegundo;
  - `503` en readiness durante el apagado y cierre del pool.
- **Cobertura honesta:** se mide sobre todo `src/`, no solo sobre los archivos que importan las pruebas.

---

## Resultados medidos

### Tamaño de imagen (T11)

| Imagen | Estrategia | Tamaño |
|---|---|---|
| `pagolab-api:naive` | Una sola etapa con dependencias de desarrollo | 2,21 GB |
| `pagolab-payments-api` | Multi-stage: `deps` → `build` → `runtime` sobre `node:22-alpine`, `npm prune --omit=dev`, usuario `node` | **375 MB** |

Reducción aproximada del **83 %**.

### Paginación con un millón de filas (T12)

Consulta de la primera página de un comercio sobre 1.000.000 de pagos (`db/seed/seed_1m.sql`):

```sql
SELECT * FROM payments
 WHERE merchant_id = 'merchant_7'
 ORDER BY created_at DESC, id DESC
 LIMIT 20;
```

| | Sin índice | Con `idx_payments_merchant_created` |
|---|---|---|
| Plan | `Parallel Seq Scan` + `top-N heapsort` | `Index Scan` (sin ordenamiento) |
| Bloques leídos | 14.154 | 23 |
| Tiempo de ejecución | 149,9 ms | **1,2 ms** |

```sql
CREATE INDEX CONCURRENTLY idx_payments_merchant_created
  ON payments (merchant_id, created_at DESC, id DESC);
```

- El índice entrega las filas ya ordenadas, así que Postgres lee solo las 20 que necesita.
- `CONCURRENTLY` permite crearlo sin bloquear escrituras; por eso la migración se declara con `transaction:false`.
- Con cursor, las páginas siguientes usan el mismo índice y mantienen el tiempo constante. Con `OFFSET`, en cambio, el tiempo crece con cada página.

---

## Decisiones y limitaciones

### Decisiones

- **SQL explícito en lugar de ORM:** control total sobre bloqueos, transiciones condicionales y planes de ejecución.
- **`UNKNOWN` como estado de primera clase:** ante la duda, nunca se declara fallido un cobro que pudo haber ocurrido.
- **Cursor opaco basado en el id:** la posición se reconstruye en la base de datos, con precisión de microsegundos, mediante una subconsulta. Así no se pierden pagos que comparten el mismo milisegundo.
- **Llamada a la pasarela fuera de transacciones:** el resolver reclama un lote en una transacción muy corta y consulta la pasarela sin bloqueos abiertos.

### Limitaciones conocidas (abordadas en fases posteriores)

- **Claves de idempotencia huérfanas:** si el proceso muere durante el cobro, la clave queda `IN_PROGRESS` y los reintentos reciben `409` hasta que expira (24 h). *Se resuelve en T14.*
- **Autenticación temporal:** `X-Merchant-Id` simula la identidad del comercio sin verificarla. *Se reemplaza por OAuth2/JWT en la Fase 3.*
- **Pasarela en memoria:** `gateway-sim` pierde sus cobros al reiniciarse, lo que puede llevar a `FAILED` pagos que sí se cobraron.
- **Sin notificación a comercios:** los cambios de estado no se publican hacia afuera. *Se incorpora con Kafka y webhooks en la Fase 2.*

El detalle del diseño está en [`specs/fase-1/design.md`](specs/fase-1/design.md).

---

## Guion de demostración (5 minutos)

Requisitos: el stack levantado (`docker compose up -d --build`) y una terminal en la raíz del repositorio.

```bash
API=localhost:3000
M="X-Merchant-Id: merchant_demo"
J="Content-Type: application/json"
KEY=$(node -e "console.log(crypto.randomUUID())")
```

**1. Crear un pago y repetirlo con la misma clave (replay)**

```bash
curl -i -X POST $API/payments -H "$J" -H "$M" -H "Idempotency-Key: $KEY" \
  -d '{"amountCents":15000,"currency":"COP"}'
# Repetir exactamente el mismo comando
```

La segunda respuesta es idéntica e incluye `Idempotent-Replayed: true`. La pasarela no vuelve a cobrar.

**2. Misma clave con otro monto (`422`)**

```bash
curl -i -X POST $API/payments -H "$J" -H "$M" -H "Idempotency-Key: $KEY" \
  -d '{"amountCents":99999,"currency":"COP"}'
```

**3. Forzar un timeout y ver la resolución automática**

```bash
SLOW_RATE=1 docker compose up -d gateway-sim      # toda respuesta tarda 5 s (> timeout de 2 s)

KEY2=$(node -e "console.log(crypto.randomUUID())")
curl -i -X POST $API/payments -H "$J" -H "$M" -H "Idempotency-Key: $KEY2" \
  -d '{"amountCents":20000,"currency":"COP"}'
# → 202 Accepted con "status": "UNKNOWN"

docker compose logs -f payments-api               # en ≤ 10 s: "Pago … resuelto como APPROVED/DECLINED"
```

Después de ver la resolución, restaura la pasarela con `docker compose up -d gateway-sim`. Hazlo solo **después**: la pasarela guarda sus cobros en memoria y los olvida al reiniciarse.

**4. Auditoría en `payment_events`**

```bash
docker compose exec postgres psql -U pagolab -d pagolab -c \
  "SELECT payment_id, from_status, to_status, reason, created_at
     FROM payment_events ORDER BY id DESC LIMIT 6;"
```

Se observa la secuencia `PENDING → PROCESSING → UNKNOWN → APPROVED`, con su motivo (`TimeoutError`, `resuelto por consulta a la pasarela`).

**5. `EXPLAIN` antes y después del índice**

Requiere el seed (`docker compose exec -T postgres psql -U pagolab -d pagolab < db/seed/seed_1m.sql`, una sola vez).

```bash
docker compose exec -T postgres psql -U pagolab -d pagolab <<'SQL'
BEGIN;
DROP INDEX idx_payments_merchant_created;          -- se revierte con el ROLLBACK
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM payments
 WHERE merchant_id = 'merchant_7' ORDER BY created_at DESC, id DESC LIMIT 20;
ROLLBACK;
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM payments
 WHERE merchant_id = 'merchant_7' ORDER BY created_at DESC, id DESC LIMIT 20;
SQL
```

---

## Historial de construcción

La Fase 1 se construyó en 14 tickets, cada uno en su rama y su *pull request*:

| Ticket | Entrega |
|---|---|
| T00 | Especificación EARS y contrato OpenAPI *design-first* |
| T01 | PostgreSQL en Docker Compose con *healthcheck* |
| T02 | Migraciones con dbmate y esquema de pagos |
| T03 | Base de `payments-api`: health, apagado ordenado, errores RFC 9457 y Swagger |
| T04 | Máquina de estados de pagos con pruebas |
| T05 | Crear y consultar pagos con transición condicional |
| T06 | `Idempotency-Key` con replay, `409` y `422` |
| T07 | Pasarela simulada con fallos, lentitud y consulta |
| T08 | Flujo de cobro con timeout y estado `UNKNOWN` |
| T09 | Resolver de pagos `UNKNOWN` con `SKIP LOCKED` y *lease* |
| T10 | Integración con Testcontainers y cobertura mínima del 85 % |
| T11 | Dockerfile multi-stage y stack completo en Compose |
| T12 | Paginación por cursor y laboratorio de `EXPLAIN ANALYZE` |
| T13 | README, diseño final y guion de demostración |

---

## Hoja de ruta

| Fase | Tema | Contenido principal |
|---|---|---|
| **1** ✅ | Núcleo de pagos + Docker | Idempotencia, máquina de estados, resolver concurrente, Testcontainers, multi-stage |
| **2** | Eventos con Kafka | Recuperación de claves huérfanas, Kafka en KRaft, *transactional outbox*, webhooks firmados con HMAC, *retry topic* y DLT, consumidores idempotentes, comparación con SQS (ElasticMQ) |
| **3** | Server MCP + OAuth2 | Keycloak, Authorization Code + PKCE, JWT con *scopes*, server MCP con tools de lectura y escritura, confirmación humana, defensas contra *prompt injection* |
| **4** | IA aplicada | Agente con Anthropic SDK y *tool use*, pgvector, búsqueda híbrida, RAG como tool MCP, sistema de evaluaciones |
| **5** | Observabilidad | Prometheus, Grafana, Loki, Tempo, OpenTelemetry, SLO con alertas por *burn rate*, *circuit breaker* |
| **6** | Kubernetes local | kind, Deployments con *probes*, HPA, PDB, Gateway API, fallos típicos provocados, Helm |
| **7** | Terraform, DynamoDB y CI | IaC con *state* y *drift*, idempotencia sobre DynamoDB, modelado *single-table*, CI/CD |

---

## Documentación

- [`specs/fase-1/requirements.md`](specs/fase-1/requirements.md): requisitos en formato EARS (R1–R10)
- [`specs/fase-1/design.md`](specs/fase-1/design.md): diseño técnico de la fase
- [`apps/payments-api/docs/openapi.yaml`](apps/payments-api/docs/openapi.yaml): contrato de la API
