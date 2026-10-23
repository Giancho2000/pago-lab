# 1. Dependencias: se cachea mientras no cambie package*.json
FROM node:22-alpine AS deps
WORKDIR /app
COPY package*.json ./
RUN npm ci

# 2. Compilación, y luego quitar las devDependencies
FROM deps AS build
COPY . .
RUN npm run build && npm prune --omit=dev

# 3. Imagen final: solo node_modules de producción + dist
FROM node:22-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
COPY --from=build --chown=node:node /app/package.json ./
USER node
CMD ["node", "dist/main.js"]