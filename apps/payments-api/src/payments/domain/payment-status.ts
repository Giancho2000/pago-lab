export enum PaymentStatus {
  PENDING = 'PENDING',
  PROCESSING = 'PROCESSING',
  APPROVED = 'APPROVED',
  DECLINED = 'DECLINED',
  UNKNOWN = 'UNKNOWN',
  FAILED = 'FAILED',
}

const TRANSITIONS: Record<PaymentStatus, PaymentStatus[]> = {
  [PaymentStatus.PENDING]: [PaymentStatus.PROCESSING],
  [PaymentStatus.PROCESSING]: [
    PaymentStatus.APPROVED,
    PaymentStatus.DECLINED,
    PaymentStatus.UNKNOWN,
    PaymentStatus.FAILED,
  ],
  [PaymentStatus.UNKNOWN]: [PaymentStatus.APPROVED, PaymentStatus.DECLINED, PaymentStatus.FAILED],
  [PaymentStatus.APPROVED]: [],
  [PaymentStatus.DECLINED]: [],
  [PaymentStatus.FAILED]: [],
};

export function canTransition(from: PaymentStatus, to: PaymentStatus): boolean {
  return TRANSITIONS[from].includes(to);
}

export function isFinal(status: PaymentStatus): boolean {
  return TRANSITIONS[status].length === 0;
}