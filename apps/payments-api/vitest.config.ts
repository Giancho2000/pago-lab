import { defineConfig } from 'vitest/config';

export default defineConfig({
  // Resolves the path aliases declared in tsconfig.json, including the ones
  // added by `nest g library`.
  test: {
    globals: true,
    root: './',
    coverage: {
      provider: 'v8',
      // Without `include`, v8 only reports files a test imported, so untested
      // files are invisible and coverage looks higher than it really is.
      include: ['src/**/*.ts'],
      exclude: ['src/**/*.spec.ts', 'src/main.ts'],
      reporter: ['text', 'html'],
      thresholds: { lines: 85 },
    },
    projects: [
      {
        extends: true,
        test: { name: 'unit', include: ['src/**/*.spec.ts'] },
      },
      {
        extends: true,
        test: {
          name: 'e2e',
          include: ['test/**/*.e2e-spec.ts'],
          globalSetup: ['./test/support/global-setup.ts'],
          // Todos los archivos comparten la misma BD (y el resolver toma pagos de cualquier comercio)
          fileParallelism: false,
          hookTimeout: 120_000,
          testTimeout: 20_000,
        },
      },
    ],
  },
});
