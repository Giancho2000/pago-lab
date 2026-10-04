import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { configureApp } from './app.setup.js';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();// esta escuchando a SIGTERM/SIGINT y ejecuta los hooks de apagado

  configureApp(app);
  await app.listen(process.env.PORT ?? 3000);
}
await bootstrap();
