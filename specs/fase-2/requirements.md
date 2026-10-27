# Requisitos · Fase 2 · Eventos con Kafka

Contratos:
- Eventos: [`specs/fase-2/asyncapi.yaml`](asyncapi.yaml)
- API HTTP (sin cambios de contrato en esta fase): `apps/payments-api/docs/openapi.yaml`

Diseño: [`design.md`](design.md) · Plan de implementación: [`tasks.md`](tasks.md)

> **Numeración:** los requisitos continúan la numeración de la Fase 1 (R1–R10) para que cada identificador sea único en todo el proyecto.
>
> **Notación EARS:** *ubicuo* (THE SYSTEM SHALL), *por evento* (WHEN), *por estado* (WHILE), *no deseado* (IF … THEN) y *opcional* (WHERE).

---

## Glosario

| Término | Definición |
|---|---|
| **Evento de pago** | Mensaje inmutable que informa que un pago llegó a `APPROVED`, `DECLINED`, `FAILED` o `UNKNOWN` |
| **Outbox** | Tabla donde el evento se guarda en la misma transacción que el cambio de estado |
| **Relay** | Proceso que lee el outbox y publica sus eventos en Kafka |
| **Notifier** | Consumidor que entrega los eventos a los comercios mediante webhooks |
| **Ledger** | Consumidor que mantiene los saldos aprobados por comercio |
| **Comercio** | Receptor de webhooks; en el laboratorio lo simula `merchant-sim` |
| **Error reintentable** | Fallo transitorio: respuesta `5xx`, timeout, error de red o cualquier otra respuesta no `2xx` distinta de `400`/`401` (por ejemplo `403`, `404` o `422`) |
| **Error no reintentable** | Fallo que no se corrige repitiendo: JSON inválido, evento sin `eventId` o `data.id` UUID, o rechazo `400`/`401` del receptor |

---

## R11 · Recuperación de claves de idempotencia huérfanas
*Ticket T14 · corrige la limitación documentada en la Fase 1*

- THE SYSTEM SHALL asociar cada pago a la `Idempotency-Key` con la que se creó.
- THE SYSTEM SHALL impedir, mediante una restricción única en la base de datos, que existan dos pagos con el mismo par (`merchant_id`, `idempotency_key`).
- THE SYSTEM SHALL asignar a cada clave en `IN_PROGRESS` un tiempo máximo de bloqueo (`locked_until`) de 60 segundos.
- WHILE el bloqueo de una clave en `IN_PROGRESS` no haya vencido, THE SYSTEM SHALL responder `409` a los reintentos.
- WHEN llega una clave en `IN_PROGRESS` con el bloqueo vencido y existe un pago asociado, THE SYSTEM SHALL responder con el estado actual de ese pago y la cabecera `Idempotent-Replayed: true`, sin volver a cobrar. El código es `201` si el estado es final y `202` si no lo es.
- WHEN llega una clave en `IN_PROGRESS` con el bloqueo vencido y no existe un pago asociado, THE SYSTEM SHALL liberar la clave y procesar la petición como nueva.

## R12 · Registro transaccional de eventos (outbox)
*Ticket T16*

- WHEN un pago transita a `APPROVED`, `DECLINED`, `FAILED` o `UNKNOWN`, THE SYSTEM SHALL registrar exactamente un evento en el outbox, en la misma transacción que el cambio de estado y su registro en `payment_events`.
- THE SYSTEM SHALL no generar eventos para las transiciones hacia `PENDING` ni `PROCESSING`.
- IF la transacción del cambio de estado falla, THEN THE SYSTEM SHALL no persistir ni el cambio de estado ni el evento.

## R13 · Contrato de eventos
*Tickets T16–T17*

- THE SYSTEM SHALL publicar cada evento con los campos `eventId` (UUID único), `type`, `version`, `occurredAt` (ISO 8601 UTC) y `data`, según el contrato de [`asyncapi.yaml`](asyncapi.yaml).
- THE SYSTEM SHALL usar los tipos `payment.approved`, `payment.declined`, `payment.failed` y `payment.unknown`.
- THE SYSTEM SHALL publicar la versión `1` del contrato. Agregar campos opcionales no cambia la versión; un cambio incompatible exige una versión nueva.
- THE SYSTEM SHALL excluir del evento cualquier dato que no esté definido en el contrato.

## R14 · Topología de Kafka
*Ticket T15*

- THE SYSTEM SHALL crear los topics de forma explícita y tener desactivada la creación automática de topics.
- THE SYSTEM SHALL disponer de:
  - `payments.events`: 6 particiones y retención de 7 días;
  - `payments.events.retry`: 3 particiones;
  - `payments.events.dlt`: 1 partición y retención de 30 días.
- THE SYSTEM SHALL ofrecer un listener para los contenedores (`kafka:9092`) y otro para la máquina anfitriona (`localhost:29092`).

## R15 · Publicación de eventos en Kafka (relay)
*Ticket T17*

- WHILE existan eventos sin publicar en el outbox, THE SYSTEM SHALL publicarlos en `payments.events` en orden de creación.
- THE SYSTEM SHALL usar el id del pago como clave del mensaje, de modo que todos los eventos de un mismo pago queden en la misma partición.
- THE SYSTEM SHALL incluir en cada mensaje las cabeceras `event-id` y `event-type`.
- THE SYSTEM SHALL publicar con un productor idempotente y `acks=all`.
- THE SYSTEM SHALL marcar un evento como publicado solo después de que el broker confirme su escritura.
- IF Kafka no está disponible, THEN THE SYSTEM SHALL conservar los eventos en el outbox y publicarlos automáticamente cuando Kafka se restablezca, sin pérdidas.
- WHERE la variable `RELAY_ENABLED` vale `true`, THE SYSTEM SHALL ejecutar el relay. La variable debe estar activa en una sola instancia.
- THE SYSTEM SHALL eliminar del outbox, una vez al día, los eventos publicados hace más de 7 días.

## R16 · Entrega de webhooks firmados
*Tickets T19–T20*

- WHEN el notifier consume un evento de `payments.events`, THE SYSTEM SHALL enviarlo al webhook del comercio con `POST`, el evento JSON como cuerpo y la cabecera `PagoLab-Signature`.
- THE SYSTEM SHALL calcular la firma como `t=<unix_segundos>,v1=<hex>`, donde `<hex>` es el HMAC-SHA256 de la cadena `"<t>.<cuerpo>"` con el secreto compartido del comercio.
- THE SYSTEM SHALL abortar cada envío que supere los 5 segundos.
- THE SYSTEM SHALL registrar cada entrega exitosa y no reenviar un evento que ya conste como entregado.
- THE SYSTEM SHALL confirmar el offset de un mensaje solo después de procesarlo, es decir, de entregarlo o de derivarlo a reintento o al DLT. El offset confirmado es el del mensaje más uno.

## R17 · Verificación de webhooks en el receptor
*Ticket T18 · `merchant-sim`*

- THE SYSTEM SHALL verificar la firma sobre el cuerpo crudo recibido, sin volver a serializarlo.
- THE SYSTEM SHALL comparar firmas en tiempo constante.
- IF la marca de tiempo de la firma difiere en más de 300 segundos de la hora actual, THEN THE SYSTEM SHALL responder `400`.
- IF la firma no coincide, THEN THE SYSTEM SHALL responder `401`.
- IF la cabecera `PagoLab-Signature` falta o no tiene el formato `t=<segundos>,v1=<hex>`, THEN THE SYSTEM SHALL responder `400`.
- IF la firma es válida pero el cuerpo no es JSON válido, THEN THE SYSTEM SHALL responder `400` (nunca `5xx`, para que el emisor no lo reintente).
- IF el cuerpo es JSON válido pero no incluye `eventId` o `data.id`, THEN THE SYSTEM SHALL responder `400`.
- IF el `eventId` ya fue procesado, THEN THE SYSTEM SHALL responder `200` indicando que es un duplicado y no reprocesarlo.
- WHERE `FAIL_RATE` es mayor que 0, THE SYSTEM SHALL responder `500` a esa proporción de webhooks válidos, para simular un comercio inestable (T20).

## R18 · Reintentos y dead letter topic
*Ticket T20*

- IF la entrega falla con un error reintentable, THEN THE SYSTEM SHALL publicar el mensaje en `payments.events.retry` con su clave original y las cabeceras `attempt`, `not-before`, `last-error` y `original-topic`.
- THE SYSTEM SHALL aplicar un backoff de 5 s, 30 s y 120 s entre el primer, segundo y tercer reintento.
- IF el error es no reintentable o se agotaron los 3 reintentos, THEN THE SYSTEM SHALL publicar el mensaje en `payments.events.dlt` con la causa en `last-error`.
- THE SYSTEM SHALL evitar que un mensaje fallido bloquee el procesamiento del resto de su partición en `payments.events`.
- WHILE no se alcance el instante `not-before` de un mensaje de reintento, THE SYSTEM SHALL esperar sin perder la pertenencia al consumer group, enviando heartbeats.
- WHEN un operador ejecuta `npm run dlt:replay`, THE SYSTEM SHALL republicar en `payments.events` los mensajes del DLT, sin las cabeceras de intento, y terminar al agotarlos.

## R19 · Libro contable (ledger)
*Ticket T21*

- WHEN el ledger consume un evento `payment.approved`, THE SYSTEM SHALL sumar su monto y una unidad al saldo aprobado del comercio **en la moneda del pago** exactamente una vez.
- THE SYSTEM SHALL mantener un saldo separado por cada combinación de comercio y moneda; nunca sumará montos de monedas distintas.
- THE SYSTEM SHALL registrar el `eventId` procesado y el efecto sobre el saldo en la misma transacción.
- IF el evento ya fue procesado, THEN THE SYSTEM SHALL ignorarlo sin modificar los saldos.
- THE SYSTEM SHALL ignorar los tipos de evento distintos de `payment.approved`.
- THE SYSTEM SHALL mantener, para cada comercio y moneda, un saldo aprobado igual a la suma de los montos de sus pagos `APPROVED` que generaron un evento `payment.approved`. Los pagos anteriores al outbox (T16), como los del seed de la Fase 1, nunca se publicaron y quedan fuera del ledger.
- IF un mensaje de `payments.events` no es un evento válido (no es JSON, no tiene `eventId` UUID, no tiene `type` o es un `payment.approved` sin `merchantId`, `amountCents` entero positivo o `currency`), THEN THE SYSTEM SHALL registrarlo con nivel de error y continuar con el siguiente.
- IF falla la base de datos del ledger, THEN THE SYSTEM SHALL dejar de avanzar sin confirmar el offset y reintentar el mismo mensaje hasta que la base de datos vuelva.
- WHEN se reinician los offsets del grupo `ledger` al inicio del topic sobre tablas vacías, THE SYSTEM SHALL reconstruir los mismos saldos para los eventos que sigan dentro de la retención del topic (7 días).

## R20 · Independencia de los consumidores
*Tickets T19–T21*

- THE SYSTEM SHALL procesar cada evento de forma independiente en los consumer groups `notifier`, `notifier-retry` y `ledger`.
- THE SYSTEM SHALL aislar los datos de cada consumidor en su propio schema de base de datos (`notifier`, `ledger`).
- IF un consumidor falla o se detiene, THEN THE SYSTEM SHALL seguir publicando eventos y procesándolos en los demás grupos. El consumidor afectado retoma desde su último offset confirmado.

## R21 · Operación de los servicios de mensajería
*Tickets T17–T21*

- WHEN un servicio consumidor recibe `SIGTERM` o `SIGINT`, THE SYSTEM SHALL desconectar sus consumidores del grupo, cerrar sus productores y su pool de base de datos, y terminar.
- WHEN `payments-api` se apaga, THE SYSTEM SHALL desconectar el productor de Kafka.
- THE SYSTEM SHALL emitir en los consumidores (`notifier` y `ledger`) logs estructurados en JSON que incluyan, por cada mensaje procesado, `eventId`, partición, offset y resultado. El relay de `payments-api` registra solo los errores y la purga del outbox con el logger de Nest; los logs JSON de la API llegan en la Fase 5.

## R22 · Calidad
*Todos los tickets*

- THE SYSTEM SHALL mantener en `payments-api` una cobertura de pruebas ≥ 85 % de líneas.
- THE SYSTEM SHALL cubrir con pruebas de integración contra PostgreSQL real los requisitos R11 y R12.
- THE SYSTEM SHALL cubrir con pruebas unitarias la firma y la entrega de webhooks (R16), la clasificación de errores y la decisión entre reintento y DLT (R18), y con pruebas de integración la aplicación idempotente del ledger (R19).

## R23 · Límite de tasa por comercio
*Ticket T25 (añadido después del cierre de la fase)*

- THE SYSTEM SHALL limitar la creación de pagos por comercio con un *token bucket* de capacidad `RATE_LIMIT_BURST` (30 por defecto) que se recarga a `RATE_LIMIT_PER_SECOND` tokens por segundo (10 por defecto).
- THE SYSTEM SHALL aplicar el mismo límite con cualquier número de réplicas de la API.
- WHEN un comercio crea un pago dentro de su límite, THE SYSTEM SHALL incluir en la respuesta las cabeceras `RateLimit-Limit` y `RateLimit-Remaining`.
- IF un comercio supera su límite, THEN THE SYSTEM SHALL responder `429` en formato RFC 9457 con la cabecera `Retry-After` en segundos, sin reclamar la `Idempotency-Key` y sin llamar a la pasarela.
- IF el limitador no está disponible, THEN THE SYSTEM SHALL permitir la petición y registrar una advertencia (*fail open*).
- WHERE `RATE_LIMIT_ENABLED` es `false`, THE SYSTEM SHALL no aplicar el límite.

---

## Fuera de alcance

- Entrega *exactly-once* de webhooks: un efecto HTTP solo puede ser *at-least-once*, y el receptor deduplica.
- Productor transaccional y `isolation.level=read_committed`: se estudian en teoría en T22.
- Clúster de varios brokers, Schema Registry y cifrado TLS de Kafka.
- Registro de comercios con URL y secreto propios: hay un único comercio simulado.
- Autenticación de la API: se aborda en la Fase 3.

## Trazabilidad

| Requisito | Ticket | Verificación |
|---|---|---|
| R11 | T14 | Prueba de integración en `payments-api` |
| R12 | T16 | Prueba de integración en `payments-api` |
| R13 | T16–T17 | Prueba unitaria de `buildPaymentEvent` y validación contra `asyncapi.yaml` |
| R14 | T15 | `kafka-topics.sh --describe` y Kafka UI |
| R15 | T17 | Prueba de integración (relay) y prueba manual deteniendo Kafka |
| R16 | T19 | Pruebas unitarias de firma y entrega (`signing.spec.ts`, `deliver.spec.ts`) y logs de `merchant-sim` |
| R17 | T18 | Pruebas manuales con `curl` (`200`/`400`/`401`/duplicado) |
| R18 | T20 | Pruebas unitarias y prueba manual con `FAIL_RATE` |
| R19 | T21 | Prueba de integración (Testcontainers) y consulta de diferencias entre saldos y pagos aprobados con evento |
| R20 | T19–T22 | Laboratorio de consumer groups (`kafka-lab.md`, archivo local no versionado) |
| R21 | T17–T21 | `docker compose stop` y revisión de logs |
| R22 | Todos | `npm run test:cov` |
| R23 | T25 | Pruebas unitarias del guard, prueba de integración (`rate-limit.e2e-spec.ts`) y ráfaga con `create-payments.mjs` |
