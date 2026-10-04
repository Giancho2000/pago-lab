import { INestApplication, ValidationPipe } from '@nestjs/common';
import { SwaggerModule } from '@nestjs/swagger';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { parse } from 'yaml';
import { ProblemDetailsFilter } from './common/problem-details.filter.js';

export function configureApp(app: INestApplication) {
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true, // Si recibimos un campo que no esta en la lista del body, devolvemos 400
      transform: true,
    }),
  );
  app.useGlobalFilters(new ProblemDetailsFilter());

  // Design-first: Swagger sirve el contrato escrito a mano, no uno generado
  const document = parse(readFileSync(join(process.cwd(), 'docs/openapi.yaml'), 'utf8'));
  SwaggerModule.setup('docs', app, document);
}