# 1. Etapa Dependencias: se cache mientras no cambie el package.json
FROM node:22-alpine AS deps
WORKDIR /app
COPY package*.json ./
RUN npm ci

# 2. Etapa Compilacion
FROM deps AS build
COPy . .
RUN npm run build && npm prune --omit=dev

# 3. Etapa Imagen final minima
FROM node:22-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
COPY --from=build --chown=node:node /app/docs ./docs
COPY --from=build --chown=node:node /app/package.json ./
USER node
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD wget -qO- http://localhost:3000/health/live || exit 1
CMD ["node","dist/main.js"]