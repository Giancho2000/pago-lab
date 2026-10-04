# Requisitos · Fase 1

Contrato: apps/payments-api/docs/openapi.yaml

## R1 · Crear y cobrar un pago
WHEN un comercio envía POST /payments con cuerpo válido, header X-Merchant-Id y header Idempotency-Key
THE SYSTEM SHALL crear el pago, intentar el cobro en la pasarela y responder 201 con el estado final,
o 202 si el estado quedó UNKNOWN.

## R2 · Validación de headers
IF falta Idempotency-Key o no es un UUID THEN THE SYSTEM SHALL responder 400.
IF falta X-Merchant-Id THEN THE SYSTEM SHALL responder 401.

## R3 · Idempotencia: repetición
WHEN llega una Idempotency-Key ya completada con el mismo cuerpo
THE SYSTEM SHALL devolver la respuesta original, sin volver a cobrar, con el header Idempotent-Replayed: true.

## R4 · Idempotencia: conflictos
IF llega una Idempotency-Key ya usada con un cuerpo distinto THEN THE SYSTEM SHALL responder 422.
IF llega una Idempotency-Key cuya petición original sigue en proceso THEN THE SYSTEM SHALL responder 409.

## R5 · Máquina de estados
THE SYSTEM SHALL permitir solo transiciones válidas
(PENDING→PROCESSING; PROCESSING→APPROVED|DECLINED|UNKNOWN|FAILED; UNKNOWN→APPROVED|DECLINED|FAILED)
y registrar cada transición en payment_events.

## R6 · Timeout de la pasarela
WHEN la pasarela no responde dentro de GATEWAY_TIMEOUT_MS
THE SYSTEM SHALL marcar el pago como UNKNOWN y nunca como FAILED.

## R7 · Resolución de desconocidos
WHILE existan pagos UNKNOWN (o PROCESSING atascados por más de 2 minutos)
THE SYSTEM SHALL consultar periódicamente su estado en la pasarela y llevarlos a un estado final.
THE SYSTEM SHALL garantizar que, con varias réplicas corriendo, cada pago lo procese una sola réplica a la vez.

## R8 · Consulta y listado
WHEN un comercio consulta GET /payments/{id} de otro comercio THE SYSTEM SHALL responder 404.
WHEN un comercio lista GET /payments THE SYSTEM SHALL paginar por cursor, del más reciente al más antiguo.

## R9 · Operación
THE SYSTEM SHALL exponer /health/live y /health/ready.
WHEN el servicio recibe SIGTERM THE SYSTEM SHALL dejar de estar listo, terminar las peticiones en curso y cerrar el pool de base de datos.

## R10 · Calidad
THE SYSTEM SHALL mantener una cobertura de pruebas ≥ 85 % de líneas.
