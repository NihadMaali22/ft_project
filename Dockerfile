# ---- build stage -------------------------------------------------------------
FROM node:24-slim AS build

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --no-audit --no-fund

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# Drop dev dependencies so the runtime stage copies a lean node_modules.
RUN npm prune --omit=dev

# ---- runtime stage -----------------------------------------------------------
FROM node:24-slim AS runtime

ENV NODE_ENV=production

WORKDIR /app

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY --from=build /app/package.json ./package.json
# Migrations are plain .sql and are not emitted by tsc, so copy them explicitly.
COPY src/db/migrations ./dist/db/migrations

USER node

EXPOSE 8080

# Heap is capped well under the 256 MB cgroup limit. V8 sizes its heap from host
# memory, not the cgroup, so without this it happily grows into an OOM kill.
# Ingest buffers live off-heap, hence the conservative old-space value.
ENV NODE_OPTIONS="--max-old-space-size=128 --max-semi-space-size=16"

CMD ["node", "dist/index.js"]
