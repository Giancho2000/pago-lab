import { Ajv } from 'ajv';
import formats from 'ajv-formats';
import { readFileSync } from 'node:fs';
import { parse } from 'yaml';

/**
 * Valida contra el contrato real (specs/fase-2/asyncapi.yaml), no contra una copia:
 * si el contrato o el código cambian sin el otro, la prueba falla.
 */
const spec = parse(
  readFileSync(new URL('../../../../specs/fase-2/asyncapi.yaml', import.meta.url), 'utf8'),
);

// strict: false porque el documento AsyncAPI trae claves que no son de JSON Schema
const ajv = new Ajv({ strict: false, allErrors: true });
// ajv-formats es CommonJS: con nodenext la función llega en .default
formats.default(ajv);
ajv.addSchema({ $id: 'asyncapi', components: spec.components });

const validate = ajv.getSchema('asyncapi#/components/schemas/PaymentEvent')!;

/** Devuelve los errores de validación del evento; lista vacía si cumple el contrato */
export function contractErrors(event: unknown): string[] {
  return validate(event)
    ? []
    : (validate.errors ?? []).map((e) => `${e.instancePath || '/'} ${e.message}`);
}
