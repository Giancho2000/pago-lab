# PagoLab

[![Licencia: MIT](https://img.shields.io/badge/licencia-MIT-blue.svg)](LICENSE)
![Node.js 22](https://img.shields.io/badge/Node.js-22-339933?logo=node.js&logoColor=white)
![TypeScript](https://img.shields.io/badge/TypeScript-ESM-3178C6?logo=typescript&logoColor=white)
![PostgreSQL 17](https://img.shields.io/badge/PostgreSQL-17-4169E1?logo=postgresql&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9%20KRaft-231F20?logo=apachekafka&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

**PagoLab** es un simulador de procesamiento de pagos **open source**. Lo construyo como laboratorio para aplicar prácticas de backend de nivel productivo en un dominio donde los errores cuestan dinero: cobros duplicados, pasarelas que no responden, eventos que se pierden y estados que nadie sabe resolver.

El proyecto sigue un enfoque **Spec-Driven Development (SDD)**: primero se escriben los requisitos (EARS) y los contratos (OpenAPI para la API, AsyncAPI para los eventos), y después el código. Cada ticket es una rama y un *pull request* con su propio historial.

**Estado actual:** las fases 1 (núcleo de pagos) y 2 (eventos con Kafka) están completas. Cada cambio de estado de un pago se publica en Kafka mediante un *transactional outbox*, y dos consumidores independientes leen esos eventos:
- **`notifier`** los entrega al comercio como webhook firmado con HMAC. Si la entrega falla, la reintenta con *backoff* (5 s, 30 s, 120 s) en un topic aparte, y lo que no se puede entregar termina en un *dead letter topic* que se puede reprocesar.
- **`ledger`** lleva los saldos aprobados por comercio y moneda con **efecto exactamente una vez**.

La Fase 3 (OAuth2 y server MCP) es la siguiente. El plan completo está en la [hoja de ruta](#hoja-de-ruta).

![Un pago recorre el sistema: evento en Kafka, webhook firmado y saldo en el ledger](docs/img/demo.gif)

---

## Contenido

1. [Qué demuestra este proyecto](#qué-demuestra-este-proyecto)
2. [Arquitectura](#arquitectura)
3. [Stack tecnológico](#stack-tecnológico)
4. [Estructura del repositorio](#estructura-del-repositorio)
5. [Puesta en marcha](#puesta-en-marcha)
6. [Uso de la API](#uso-de-la-api)
7. [Eventos y webhooks](#eventos-y-webhooks)
8. [Pruebas y cobertura](#pruebas-y-cobertura)
9. [Resultados medidos](#resultados-medidos)
10. [Decisiones y limitaciones](#decisiones-y-limitaciones)
11. [Guion de demostración (5 minutos)](#guion-de-demostración-5-minutos)
12. [Historial de construcción](#historial-de-construcción)
13. [Hoja de ruta](#hoja-de-ruta)
14. [Documentación](#documentación)
15. [Contribuir](#contribuir)
16. [Licencia](#licencia)

---

## Qué demuestra este proyecto

| Tema | Cómo se resuelve en PagoLab |
|---|---|
| **Idempotencia** | Cabecera `Idempotency-Key` con reclamo atómico (`INSERT … ON CONFLICT DO NOTHING`). Si se repite la petición, devuelve la respuesta original con `Idempotent-Replayed: true`. Responde `422` si el cuerpo es distinto y `409` si la petición original sigue en curso. |
| **Límite de tasa por comercio** | *Token bucket* en PostgreSQL (ráfaga de 30 y 10 creaciones por segundo, configurables) con una sola sentencia atómica, así que el límite es el mismo con cualquier número de réplicas. Responde `429` con `Retry-After` **antes** de reclamar la `Idempotency-Key`, de modo que reintentar con la misma clave es seguro. |
| **Prevención de doble cobro** | El id del pago viaja como clave de idempotencia hacia la pasarela, y cada cambio de estado es un `UPDATE … WHERE status = $from`: si otro proceso se adelantó, la operación no tiene efecto. |
| **Timeouts honestos** | Si la pasarela no responde dentro de `GATEWAY_TIMEOUT_MS`, el pago queda en `UNKNOWN` (`202`), **nunca** en `FAILED`: no hay certeza de que el cobro no ocurrió. |
| **Recuperación de claves huérfanas** | Si el proceso muere a mitad de un cobro, la clave queda `IN_PROGRESS`. Pasado `locked_until`, un reintento la recupera: si el pago ya existe devuelve ese mismo pago (sin volver a cobrar) y, si no, la procesa como nueva. Un índice único `(merchant_id, idempotency_key)` es la segunda barrera en la base de datos. |
| **Resolución concurrente segura** | Un proceso periódico consulta a la pasarela los pagos `UNKNOWN` y los `PROCESSING` atascados. Usa `FOR UPDATE SKIP LOCKED` y un *lease* (`next_check_at`), así que cada pago lo procesa una sola réplica a la vez. |
| **Máquina de estados auditable** | Solo se permiten transiciones válidas, y cada una queda registrada en `payment_events` dentro de la misma transacción. |
| **Transactional outbox** | El cambio de estado y el evento se guardan en la **misma transacción** (`outbox`). No puede haber un pago aprobado sin evento, ni un evento de un cambio que se revirtió. |
| **Publicación confiable en Kafka** | Un relay lee el outbox con `FOR UPDATE SKIP LOCKED` y publica con un productor idempotente (`acks=all`) usando `paymentId` como clave, así que los eventos de un mismo pago conservan su orden. Si Kafka cae, los eventos esperan en el outbox y se publican al volver. |
| **Webhooks firmados** | El comercio recibe cada evento con la cabecera `PagoLab-Signature: t=…,v1=HMAC-SHA256`. El receptor verifica sobre el cuerpo crudo, compara en tiempo constante, rechaza firmas de más de 5 minutos (*replay*) y deduplica por `eventId`. |
| **Consumidor idempotente** | El `notifier` confirma el offset de Kafka a mano y solo después de entregar (*at-least-once*). Registra cada entrega en su propio schema, así que un reinicio o un rebalanceo no reenvía lo ya entregado. |
| **Reintentos sin bloqueo y DLT** | Un webhook fallido no detiene la partición: se deriva a un *retry topic* con *backoff* de 5 s, 30 s y 120 s, o directo al DLT si el error no tiene arreglo (`400`/`401`, mensaje corrupto). El DLT conserva la causa en `last-error` y se reprocesa con `npm run dlt:replay`. |
| **Efecto exactamente una vez** | El `ledger` registra el `eventId` y suma el saldo **en la misma transacción**: un evento repetido (rebalanceo, replay) nunca suma dos veces, y dos réplicas que procesan el mismo evento a la vez se ordenan por el bloqueo de la PK. Vaciando sus tablas y rebobinando los offsets del grupo, reconstruye los mismos saldos desde Kafka. |
| **Consumidores independientes** | `notifier` y `ledger` leen el mismo topic con consumer groups distintos: cada uno avanza a su ritmo, y si uno se detiene el otro sigue. Cada servicio es dueño de su propio schema de base de datos. |
| **Paginación eficiente** | Paginación por cursor (*keyset*) sobre `(created_at, id)` con un índice compuesto. El tiempo de respuesta no se degrada al avanzar páginas. |
| **Operación** | Endpoints `/health/live` y `/health/ready`, apagado ordenado ante `SIGTERM` (deja de estar listo, termina las peticiones en curso y cierra el pool) y errores en formato **RFC 9457** (`application/problem+json`). |
| **Pruebas contra infraestructura real** | Pruebas de integración contra un PostgreSQL real con **Testcontainers**, una pasarela falsa determinista y escenarios de concurrencia. Los consumidores tienen pruebas propias de firma, entrega, reintentos y aplicación exactamente una vez. |
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
  REPO -->|"estado + evento<br/>misma transacción"| PG[(PostgreSQL 17<br/>payments · payment_events<br/>idempotency_keys · outbox)]
  MIG[dbmate] -->|migraciones| PG

  RELAY[OutboxRelay<br/>dentro de payments-api] -->|"SKIP LOCKED"| PG
  RELAY -->|"key = paymentId"| K[[Kafka · payments.events<br/>6 particiones]]
  K -->|"group: notifier"| N[notifier]
  N -->|"POST firmado HMAC"| MS[merchant-sim]
  N --> ND[(notifier.deliveries)]
  N -->|"500 / timeout"| RT[[payments.events.retry]]
  RT -->|"group: notifier-retry<br/>espera not-before"| N
  N -->|"400 / 401 / no JSON<br/>o 4.º fallo"| DLT[[payments.events.dlt]]
  DLT -.->|"npm run dlt:replay"| K
  K -->|"group: ledger"| L[ledger]
  L -->|"marca + saldo<br/>misma transacción"| LB[(ledger.processed_events<br/>ledger.merchant_balances)]
```

El diseño completo de la Fase 2 está en [`specs/fase-2/design.md`](specs/fase-2/design.md).

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
| `payments-api` | API de pagos (NestJS) y relay del outbox hacia Kafka | `3000` |
| `kafka` | Broker Kafka 3.9 en modo KRaft (sin ZooKeeper) | `29092` |
| `kafka-init` | Crea los topics (`infra/kafka/create-topics.sh`) y termina | — |
| `kafka-ui` | Interfaz web para inspeccionar topics, mensajes y consumer groups | `8081` |
| `merchant-sim` | Comercio simulado (Express) que verifica las firmas HMAC; `FAIL_RATE` simula fallos | `4100` |
| `notifier` | Consume `payments.events` y entrega los webhooks al comercio, con reintentos y DLT | — (sin puerto, para poder escalarlo) |
| `ledger` | Consume `payments.events` y mantiene los saldos aprobados por comercio y moneda | — (sin puerto, para poder escalarlo) |

---

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Lenguaje y runtime | TypeScript 6 · Node.js 22 (ESM) |
| Framework | NestJS 12 (`@nestjs/schedule`, `@nestjs/swagger`, `class-validator`) |
| Base de datos | PostgreSQL 17 · driver `pg` (SQL explícito, sin ORM) |
| Migraciones | dbmate 2 |
| Mensajería | Apache Kafka 3.9 (KRaft) · `kafkajs` · Kafka UI |
| Servicios auxiliares | Express 5 (`gateway-sim`, `merchant-sim`) · TypeScript + `pino` para logs JSON (`notifier`) |
| Pruebas | Vitest · Supertest · Testcontainers · cobertura con V8 |
| Calidad | oxlint · Prettier |
| Contenedores | Docker (multi-stage) · Docker Compose |
| Contratos | OpenAPI 3 (*design-first*), servido con Swagger UI · AsyncAPI para los eventos |

---

## Estructura del repositorio

```text
pagos-lab/
├── apps/
│   ├── payments-api/            # API de pagos (NestJS)
│   │   ├── docs/openapi.yaml    # Contrato de la API (fuente de verdad)
│   │   ├── src/
│   │   │   ├── common/          # Filtro RFC 9457, transacciones, X-Merchant-Id y rate limit
│   │   │   ├── database/        # Pool de PostgreSQL y cierre ordenado
│   │   │   ├── health/          # Liveness y readiness
│   │   │   ├── messaging/       # Productor de Kafka y relay del outbox
│   │   │   └── payments/        # Dominio, controlador, servicio, repositorio,
│   │   │                        # idempotencia, cliente de pasarela y resolver
│   │   ├── test/                # Pruebas e2e (Testcontainers + pasarela falsa)
│   │   └── Dockerfile
│   ├── gateway-sim/             # Pasarela de pagos simulada
│   ├── merchant-sim/            # Comercio simulado que verifica webhooks
│   │   └── scripts/check-webhook.sh   # Verificación manual de firmas con curl + openssl
│   ├── notifier/                # Consumidor de Kafka que entrega webhooks
│   │   ├── src/                 # main.ts (consumidores), deliver.ts, signing.ts,
│   │   │                        # retry.ts (retry/DLT) y dlt-replay.ts
│   │   └── test/                # Pruebas unitarias de firma, entrega y retry/DLT
│   └── ledger/                  # Consumidor de Kafka que lleva los saldos
│       ├── src/                 # main.ts (consumidor) y apply.ts (exactamente una vez)
│       └── test/                # Pruebas de integración contra PostgreSQL real
├── db/
│   ├── migrations/              # Migraciones de dbmate
│   └── seed/seed_1m.sql         # 1.000.000 de pagos para el laboratorio de EXPLAIN
├── infra/kafka/                 # Script de creación de topics
├── scripts/create-payments.mjs  # Crea N pagos de prueba contra la API local
├── specs/
│   ├── fase-1/                  # Requisitos (EARS) y diseño
│   └── fase-2/                  # Requisitos, diseño, AsyncAPI y plan por ticket
└── docker-compose.yml
```

---

## Puesta en marcha

### Requisitos previos

- Docker Desktop (o Docker Engine con Compose v2)
- Node.js 22 y npm, solo para ejecutar las pruebas o la API fuera de Docker
- Git Bash, WSL o una terminal POSIX para los comandos de este documento

> **Git Bash en Windows:** dentro de `$(...)` escribe `node.exe` en lugar de `node`. Git Bash define `node` como un alias de `winpty node.exe`, y `winpty` falla con `stdout is not a tty` cuando su salida se captura. En ese caso `$(...)` queda vacío, por ejemplo una `Idempotency-Key` vacía que la API rechaza con `400`.

### Levantar el stack completo

```bash
git clone https://github.com/Giancho2000/pago-lab.git
cd pago-lab
docker compose up -d --build
```

Compose arranca PostgreSQL y Kafka, espera sus *healthchecks*, aplica las migraciones y crea los topics. Después inicia la pasarela, el comercio, la API y los consumidores (`notifier` y `ledger`).

> Las credenciales del `docker-compose.yml` (`pagolab`/`pagolab` en PostgreSQL, `whsec_local` como secreto de webhooks) son **solo para desarrollo local**. En cualquier otro entorno deben venir de variables de entorno o de un gestor de secretos.

### Verificar

```bash
docker compose ps -a                      # migrate y kafka-init en "Exited (0)"; el resto "running"/"healthy"
curl localhost:3000/health/live           # {"status":"ok"}
curl localhost:3000/health/ready          # {"status":"ready"}
```

Si `payments-api`, `notifier` o `ledger` se quedan en estado `Created`, es porque algo de lo que dependen falló. Lo más común es que `migrate` termine con un error: `docker compose logs migrate` muestra la línea exacta.

- Documentación interactiva de la API: **http://localhost:3000/docs**
- Kafka UI (topics, mensajes, consumer groups): **http://localhost:8081**

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
| `KAFKA_BROKERS` | Brokers de Kafka | `localhost:29092` (en Compose: `kafka:9092`) |
| `RELAY_ENABLED` | Activa el relay del outbox. Debe estar activo en **una sola** instancia para conservar el orden de los eventos | `true` |
| `RATE_LIMIT_ENABLED` | Activa el límite de creación de pagos por comercio | `true` |
| `RATE_LIMIT_BURST` | Capacidad del *token bucket*: ráfaga máxima de creaciones por comercio | `30` |
| `RATE_LIMIT_PER_SECOND` | Tokens que se recargan por segundo (ritmo sostenido por comercio) | `10` |

### Detener

```bash
docker compose down        # conserva los datos
docker compose down -v     # elimina también los volúmenes de PostgreSQL y Kafka
```

---

## Uso de la API

Todas las peticiones requieren la cabecera `X-Merchant-Id`, que identifica al comercio (es un mecanismo temporal; ver [limitaciones](#decisiones-y-limitaciones)).

| Método | Ruta | Descripción | Respuestas |
|---|---|---|---|
| `POST` | `/payments` | Crea un pago y lo cobra en la pasarela. Requiere `Idempotency-Key` (UUID). Limitado por comercio | `201` estado final · `202` `UNKNOWN` · `400` · `401` · `409` · `422` · `429` |
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

### Límite de tasa

La creación de pagos está limitada **por comercio** con un *token bucket*: cada comercio tiene un balde de 30 tokens que se recarga a 10 por segundo. Cada `POST /payments` gasta un token; un doble clic o un lote corto pasan sin problema, pero un cliente en bucle recibe `429`.

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1
RateLimit-Limit: 30
RateLimit-Remaining: 0
Content-Type: application/problem+json

{ "type": "about:blank", "title": "TOO_MANY_REQUESTS", "status": 429,
  "detail": "Límite de creación de pagos superado para el comercio; reintenta en 1 s", "instance": "/payments" }
```

- Las respuestas `201`/`202` incluyen `RateLimit-Limit` y `RateLimit-Remaining` para que el cliente regule su ritmo.
- El `429` ocurre antes de reclamar la `Idempotency-Key` y de llamar a la pasarela: no queda rastro, así que se reintenta con la misma clave tras `Retry-After`.
- Para verlo: `node scripts/create-payments.mjs 40` envía 40 pagos simultáneos del mismo comercio; unos 30 pasan y el resto recibe `429`.

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

## Eventos y webhooks

Cada vez que un pago llega a un estado de interés para el comercio, se genera un evento:

| Estado | Evento |
|---|---|
| `APPROVED` | `payment.approved` |
| `DECLINED` | `payment.declined` |
| `FAILED` | `payment.failed` |
| `UNKNOWN` | `payment.unknown` |

`PENDING` y `PROCESSING` son estados internos y no generan evento. El contrato completo está en [`specs/fase-2/asyncapi.yaml`](specs/fase-2/asyncapi.yaml):

```json
{
  "eventId": "0b5c6f0e-3a51-4d36-9e0e-8a2f4d3c1b7a",
  "type": "payment.approved",
  "version": 1,
  "occurredAt": "2026-10-14T15:04:05.123Z",
  "data": {
    "id": "7c1e2a9b-…",
    "merchantId": "merchant_demo",
    "amountCents": 15000,
    "currency": "COP",
    "status": "APPROVED",
    "gatewayRef": "ch_8f3a2b1c"
  }
}
```

### Recorrido de un evento

1. **payments-api** cambia el estado del pago e inserta el evento en `outbox`, en la misma transacción.
2. **OutboxRelay** (cada 1 s) publica los pendientes en `payments.events` con `key = paymentId` y los marca como publicados.
3. **notifier** (consumer group `notifier`) lee el evento, revisa en `notifier.deliveries` si ya lo entregó y, si no, lo envía al comercio con `POST` y la cabecera `PagoLab-Signature` (timeout de 5 s).
4. Si el comercio responde `2xx`, registra la entrega. Si falla, deriva el mensaje (ver la tabla siguiente). En ambos casos confirma el offset (`offset + 1`) **después**: el mensaje ya quedó entregado o guardado en otro topic, así que la partición sigue avanzando.
5. **merchant-sim** verifica la firma y responde `200`. Si el `eventId` ya lo había recibido, responde `200` como duplicado sin reprocesarlo.

### Reintentos y dead letter topic

| Fallo | Ejemplo | Destino |
|---|---|---|
| Reintentable | `5xx`, otro `4xx` (`403`, `404`…), timeout, comercio caído | `payments.events.retry`, con espera de 5 s, 30 s y 120 s |
| No reintentable | `400`, `401`, mensaje no JSON o sin `eventId`/`data.id` | `payments.events.dlt`, directo |
| Reintentos agotados | 4.º fallo consecutivo | `payments.events.dlt` |

- **Cabeceras:** el mensaje derivado conserva su clave (`paymentId`), su cuerpo y sus cabeceras originales, y agrega `attempt`, `not-before`, `last-error` y `original-topic`.
- **Espera sin perder el grupo:** el grupo `notifier-retry` espera hasta `not-before` enviando *heartbeats*, para que Kafka no lo dé por muerto.
- **Reproceso:** tras corregir la causa, `npm run dlt:replay` republica el DLT en `payments.events` sin las cabeceras de intento y termina al agotarlo.

¿Por qué un topic aparte y no reintentar en el mismo lugar? Esperar 2 minutos dentro del consumidor principal bloquearía la partición entera, y con ella los webhooks de muchos otros pagos (*head-of-line blocking*).

### Garantías

| Tramo | Garantía | Si algo muere a mitad |
|---|---|---|
| Cambio de estado → outbox | Atómica | No queda ni el cambio ni el evento |
| Outbox → Kafka | Al menos una vez | El lote se publica de nuevo con los mismos `eventId` |
| Kafka → notifier → comercio | Al menos una vez | El webhook se reenvía y el comercio lo deduplica por `eventId` |
| Kafka → ledger | **Efecto exactamente una vez** | Si muere antes del `COMMIT`, no queda nada; si muere después, el reproceso encuentra la marca y no vuelve a sumar |

**La diferencia clave:** un webhook HTTP **no puede** ser *exactly-once*, porque siempre existe una ventana entre "el comercio recibió" y "registré que recibió", y la última barrera es la deduplicación en el receptor. En el ledger, el efecto (el saldo) y la marca de procesado viven en la **misma base de datos**, así que una transacción los confirma juntos.

### Ledger

```bash
docker compose logs -f ledger      # "applied", "ignored" (otros tipos), "duplicate", "invalid"
docker compose exec postgres psql -U pagolab -d pagolab -c "SELECT * FROM ledger.merchant_balances;"
```

Para comprobar que los saldos coinciden con los pagos aprobados, usa la consulta de diferencias de [`specs/fase-2/tasks.md`](specs/fase-2/tasks.md) (T21): debe devolver `0 rows`.

**Reconstruir los saldos desde Kafka:**

```bash
docker compose stop ledger         # Kafka no permite mover offsets de un grupo con miembros activos
docker compose exec postgres psql -U pagolab -d pagolab \
  -c "TRUNCATE ledger.processed_events, ledger.merchant_balances;"
MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group ledger \
  --reset-offsets --to-earliest --topic payments.events --execute
docker compose start ledger        # relee el topic y reconstruye los mismos saldos
```

### Verlo funcionando

```bash
FAIL_RATE=0 docker compose up -d --build          # comercio sin fallos simulados
docker compose logs -f notifier merchant-sim      # en otra terminal

node scripts/create-payments.mjs 5                # crea 5 pagos
```

Por cada pago aparece una línea `"result":"delivered"` en el notifier y una `OK payment.approved pago=…` (o `declined`) en merchant-sim.

```bash
# Entregas registradas
docker compose exec postgres psql -U pagolab -d pagolab -c "SELECT count(*) FROM notifier.deliveries;"

# Offsets confirmados por partición; LAG = 0 significa que el notifier está al día
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group notifier

# Reiniciar el notifier no reenvía lo ya entregado: no aparecen nuevos OK ni DUPLICADO
docker compose restart notifier
```

Para ver los reintentos y el DLT:

```bash
FAIL_RATE=0.5 docker compose up -d merchant-sim   # verás "result":"retry" y luego "delivered"
FAIL_RATE=1 docker compose up -d merchant-sim     # tras ~2,5 min, los mensajes llegan al DLT

# Leer el DLT con sus cabeceras (attempt, last-error…)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payments.events.dlt --from-beginning \
  --timeout-ms 5000 --property print.headers=true --property print.key=true

# Recuperar el comercio y reprocesar el DLT
FAIL_RATE=0 docker compose up -d merchant-sim
docker compose exec notifier npm run dlt:replay
```

> En Git Bash, si un comando con rutas como `/opt/kafka/...` falla con `no such file or directory` y una ruta `C:/Program Files/Git/...`, antepón `MSYS_NO_PATHCONV=1`. Git Bash convierte las rutas que empiezan con `/` en rutas de Windows.

### Ver los eventos en Kafka UI

Kafka UI (**http://localhost:8081**) muestra lo mismo que los comandos anteriores sin usar la terminal:

| Qué quieres ver | Dónde | Qué buscar |
|---|---|---|
| Los eventos de un pago | *Topics* → `payments.events` → *Messages* | La **clave** es el `paymentId`: todos los eventos de un pago caen en la misma partición y en orden. El **valor** es el JSON del contrato ([AsyncAPI](specs/fase-2/asyncapi.yaml)) |
| Reintentos en espera | *Topics* → `payments.events.retry` → *Messages* | Las **cabeceras** `attempt` (1–3), `not-before` (momento del reintento, en milisegundos *epoch*) y `last-error` |
| Mensajes que no se pudieron entregar | *Topics* → `payments.events.dlt` → *Messages* | `attempt: 4`, o un error no reintentable (`400`, `401`, mensaje no JSON) en `last-error` |
| El avance de cada consumidor | *Consumers* | Los grupos `notifier`, `notifier-retry`, `ledger` y `dlt-replay`, con el **lag** por partición. `0` significa que el grupo está al día |
| Cómo se reparten las particiones | *Consumers* → `notifier` | Con `docker compose up -d --scale notifier=3`, cada réplica toma 2 de las 6 particiones |

Prueba rápida: detén el notifier (`docker compose stop notifier`), crea 20 pagos (`node scripts/create-payments.mjs 20`) y mira en *Consumers* cómo crece el lag del grupo `notifier` mientras el del `ledger` sigue en 0. Al iniciarlo de nuevo (`docker compose start notifier`), el lag vuelve a 0 sin perder ningún evento.

### Verificar el receptor de webhooks

`apps/merchant-sim/scripts/check-webhook.sh` firma peticiones con `openssl` y comprueba las respuestas de merchant-sim: firma válida `200`, cuerpo o `t` alterados `401`, firma de hace 10 minutos `400`, cabecera ausente o mal formada `400`, cuerpo no JSON `400` y duplicado `200`.

```bash
FAIL_RATE=0 docker compose up -d merchant-sim
sh apps/merchant-sim/scripts/check-webhook.sh

# Proporción de 500 simulados
FAIL_RATE=0.2 docker compose up -d merchant-sim
N=200 sh apps/merchant-sim/scripts/check-webhook.sh
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

Desde `apps/notifier` y `apps/ledger`, `npm test` ejecuta las pruebas de cada servicio; las del ledger necesitan Docker (Testcontainers). Vitest no verifica tipos, así que conviene acompañarlo de `npx tsc --noEmit`, que es el mismo chequeo que corre el build de Docker.

**Estado actual:**
- `payments-api`: 125 pruebas en verde · **97,1 %** de cobertura de líneas.
- `notifier`: 25 pruebas unitarias (firma, entrega de webhooks y decisión entre reintento y DLT).
- `ledger`: 7 pruebas (integración de `apply` contra PostgreSQL y validación de eventos).

### Estrategia

- **Unitarias:** máquina de estados, cliente de la pasarela (respuestas `201`/`402`/`4xx`/`5xx` y timeout), ciclo del resolver y manejo de transacciones.
- **Unitarias del notifier:**
  - la firma usa segundos, firma exactamente `"<t>.<cuerpo>"` como verifica el receptor y cambia si cambia un byte del cuerpo o el secreto;
  - `deliver`, con `fetch` y pool simulados, no reenvía lo ya entregado, envía un POST cuya firma se verifica igual que en el receptor, trata `400`/`401` como no reintentables y `5xx`, otros `4xx`, timeouts y fallos al registrar la entrega como reintentables; `parseEvent` rechaza mensajes no JSON o incompletos;
  - `sendToRetryOrDlt` aplica el *backoff* correcto en cada intento, manda al DLT el 4.º fallo y los errores no reintentables, y conserva la clave, el cuerpo y las cabeceras originales.
- **Integración del ledger:** contra un PostgreSQL efímero con la migración real. Verifican que un evento repetido aplica una sola vez, que los saldos se separan por comercio y moneda, que los otros tipos se ignoran sin dejar marca, que **dos aplicaciones simultáneas del mismo evento** solo suman una vez y que, si falla la actualización del saldo, **la marca también se revierte** (todo o nada).
- **Integración (e2e):** la aplicación completa contra un PostgreSQL efímero, con las migraciones reales, una pasarela falsa determinista y un productor de Kafka falso. Las pruebas están organizadas por requisito e incluyen:
  - recuperación de claves huérfanas, con y sin pago creado (T14);
  - evento en el outbox en la misma transacción que el cambio de estado, y relay que solo marca como publicado si Kafka confirmó (T16–T17);
  - repetición idempotente, conflictos `409`/`422` y liberación de la clave ante fallos previos al cobro;
  - timeout que deja el pago en `UNKNOWN` aunque la pasarela sí haya cobrado;
  - dos resolvers concurrentes que nunca procesan el mismo pago;
  - paginación completa siguiendo `nextCursor`, con empates de `created_at` y pagos creados en el mismo milisegundo;
  - `503` en readiness durante el apagado y cierre del pool;
  - límite por comercio: `429` con `Retry-After` sin cobrar ni reclamar la clave, recarga con el tiempo, baldes independientes por comercio, consumos concurrentes que nunca gastan el mismo token y reintento seguro con la misma clave.
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
- **Outbox en lugar de publicar directo a Kafka:** escribir en la base de datos y en Kafka por separado deja una ventana en la que una de las dos operaciones puede fallar. El outbox convierte el problema en una sola transacción local más una publicación que se puede repetir.
- **Evento con estado (*event-carried state*):** el evento lleva los datos del pago, así que el comercio no necesita consultar la API para saber qué pasó.
- **Un schema por consumidor:** `notifier` es dueño de `notifier.*` y `ledger` de `ledger.*`; ninguno lee tablas de otros servicios. Comparten instancia de PostgreSQL solo por economía del laboratorio.
- **Ledger como servicio aparte:** en lugar de un segundo punto de entrada dentro del notifier, para aislar fallos y escalar cada uno por separado.
- **Rate limit en PostgreSQL y no en memoria:** un contador en memoria se multiplica por el número de réplicas (3 réplicas = 3 veces el límite). En la base de datos, una sola sentencia `INSERT … ON CONFLICT DO UPDATE … WHERE` recarga y consume de forma atómica. Si el limitador falla, la petición pasa (*fail open*): el límite protege, pero no debe ser la causa de que un pago falle.
- **Fallos distintos, tratamientos distintos:** si el comercio falla, el notifier aparta el mensaje (retry/DLT) y sigue con los demás. Si la base de datos del ledger falla, el ledger **se detiene** y reintenta: saltarse un pago aprobado dejaría un saldo incorrecto. Un mensaje ilegible, en cambio, se registra y se salta, porque nunca podrá procesarse.

### Limitaciones conocidas

- **Los reintentos rompen el orden por pago:** si el webhook de `payment.unknown` entra a reintento, el de `payment.approved` del mismo pago puede llegar antes. El comercio debe usar `occurredAt` para ignorar eventos más antiguos que el último que procesó.
- **Un solo topic de reintento:** un mensaje que espera 120 s retrasa a los que esperan 5 s detrás de él en la misma partición. La alternativa es un topic por nivel de espera (`retry-5s`, `retry-30s`, `retry-2m`).
- **Un mensaje venenoso vuelve al DLT en cada replay:** `dlt:replay` reenvía todo lo pendiente. Un mensaje que nunca podrá procesarse debe analizarse y descartarse, no reprocesarse.
- **El ledger solo conoce lo publicado:** los pagos anteriores al outbox (T16), incluido el seed de un millón de pagos, nunca generaron eventos y no forman parte de los saldos.
- **Reconstrucción limitada a la retención:** rebobinar el grupo `ledger` solo recupera lo que siga en Kafka (7 días). Para más, harían falta *snapshots* de los saldos.
- **Relay en una sola instancia:** con dos relays activos, los eventos de un mismo pago podrían publicarse fuera de orden.
- **Deduplicación del comercio en memoria:** `merchant-sim` olvida los `eventId` recibidos al reiniciarse.
- **Autenticación temporal:** `X-Merchant-Id` simula la identidad del comercio sin verificarla. *Se reemplaza por OAuth2/JWT en la Fase 3.*
- **El límite de tasa se puede esquivar mientras no haya autenticación:** como `X-Merchant-Id` no se verifica, cambiarlo en cada petición da un balde nuevo. Con OAuth2 (Fase 3) el límite se aplicará sobre una identidad confiable. Solo se limita la creación de pagos; las consultas no tienen límite.
- **Pasarela en memoria:** `gateway-sim` pierde sus cobros al reiniciarse, lo que puede llevar a `FAILED` pagos que sí se cobraron.

El detalle del diseño está en [`specs/fase-1/design.md`](specs/fase-1/design.md) y [`specs/fase-2/design.md`](specs/fase-2/design.md).

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

**6. Del pago al webhook y al saldo**

```bash
FAIL_RATE=0 docker compose up -d merchant-sim
docker compose logs -f notifier merchant-sim ledger     # en otra terminal

node scripts/create-payments.mjs 3
```

Por cada pago se ve el evento entregado (`"result":"delivered"`), el webhook verificado en merchant-sim (`OK payment.approved pago=…`) y, si fue aprobado, el saldo aplicado en el ledger (`"result":"applied"`). En Kafka UI (http://localhost:8081), el grupo `notifier` y el grupo `ledger` vuelven a lag 0.

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

La Fase 2 se construyó en estos tickets:

| Ticket | Entrega |
|---|---|
| T14 | Recuperación de claves de idempotencia huérfanas |
| T15 | Kafka en KRaft, Kafka UI y creación de topics |
| T16 | *Transactional outbox* para los eventos de pagos |
| T17 | Relay del outbox hacia Kafka con productor idempotente |
| T18 | Comercio simulado con verificación HMAC de webhooks |
| T19 | Notifier con commits manuales y entrega idempotente |
| T20 | Retry topic con backoff y DLT |
| T21 | Ledger con efecto exactamente una vez |
| T24 | Cierre de la fase: specs revisados, pruebas de entrega del notifier, Kafka UI y publicación open source |
| T25 | Límite de tasa por comercio con *token bucket* en PostgreSQL |

Los laboratorios prácticos de consumer groups (T22) y de SQS con ElasticMQ (T23) quedan como ejercicios de estudio y no forman parte del código publicado.

---

## Hoja de ruta

| Fase | Tema | Contenido principal |
|---|---|---|
| **1** ✅ | Núcleo de pagos + Docker | Idempotencia, máquina de estados, resolver concurrente, Testcontainers, multi-stage |
| **2** ✅ | Eventos con Kafka | Recuperación de claves huérfanas, Kafka en KRaft, *transactional outbox*, webhooks firmados con HMAC, *retry topic* y DLT, consumidores idempotentes, ledger con efecto exactamente una vez |
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
- [`specs/fase-2/requirements.md`](specs/fase-2/requirements.md): requisitos de la Fase 2 (R11–R22)
- [`specs/fase-2/design.md`](specs/fase-2/design.md): diseño de eventos, outbox, webhooks, reintentos y ledger
- [`specs/fase-2/asyncapi.yaml`](specs/fase-2/asyncapi.yaml): contrato de los eventos
- [`specs/fase-2/tasks.md`](specs/fase-2/tasks.md): plan de implementación por ticket

---

## Contribuir

PagoLab es un proyecto de aprendizaje abierto. Las *issues* con dudas, errores o propuestas son bienvenidas, igual que los *pull requests*.

1. Abre primero una *issue* para cambios grandes, así acordamos el enfoque antes de escribir código.
2. Crea una rama desde `develop` y mantén el estilo del proyecto: SQL explícito, pruebas contra infraestructura real (Testcontainers) y specs actualizados si cambia el comportamiento.
3. Antes del PR, ejecuta en cada servicio que modifiques `npm test && npx tsc --noEmit`, y comprueba que el stack completo levanta con `docker compose up -d --build`.

---

## Licencia

Distribuido bajo la licencia [MIT](LICENSE). Puedes usar, copiar, modificar y distribuir el código, incluso con fines comerciales, conservando el aviso de copyright.
