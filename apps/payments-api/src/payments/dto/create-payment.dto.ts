import { IsIn, IsInt, IsOptional, IsString, Max, MaxLength, Min } from 'class-validator';

export class CreatePaymentDto {
  @IsInt()
  @Min(1)
  @Max(1_000_000_000) // Monto maximo de poder transaccionar, tambien se lo limitaremos al MCP
  amountCents!: number;

  @IsIn(['COP', 'USD'])
  currency!: string;

  @IsOptional()
  @IsString()
  @MaxLength(140)
  description?: string;
}