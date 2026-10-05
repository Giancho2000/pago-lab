import { canTransition, isFinal, PaymentStatus as S } from './payment-status.js';

// Aca es importante como es el flujo de estados de un pago:  PENDING -> PROCESSING -> estadofinal [APPROVED,FAILED]

describe('Máquina de estados de pagos', () => {
  it.each([
    [S.PENDING, S.PROCESSING],
    [S.PROCESSING, S.APPROVED],
    [S.PROCESSING, S.UNKNOWN],
    [S.UNKNOWN, S.APPROVED],
    [S.UNKNOWN, S.FAILED],
  ])('permite %s → %s', (from, to) => {
    expect(canTransition(from, to)).toBe(true);
  });

  it.each([
    [S.PENDING, S.APPROVED],   // no se aprueba sin procesar
    [S.APPROVED, S.DECLINED],  // un estado final no cambia
    [S.UNKNOWN, S.PROCESSING], // no se vuelve atrás
    [S.FAILED, S.APPROVED],
  ])('rechaza %s → %s', (from, to) => {
    expect(canTransition(from, to)).toBe(false);
  });

  it('identifica los estados finales', () => {
    expect([S.APPROVED, S.DECLINED, S.FAILED].every(isFinal)).toBe(true);
    expect([S.PENDING, S.PROCESSING, S.UNKNOWN].some(isFinal)).toBe(false);
  });
});