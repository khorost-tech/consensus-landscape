# syntax=docker/dockerfile:1
#
# Consensus Landscape — статический образ для k8s (consensus.khorost.tech).
# Собирает симулятор (Vite SPA, base '/') и документацию (VitePress, base '/docs/'),
# сливает их в одно дерево и отдаёт через nginx. Отдельно от GitHub Pages-деплоя.
#
# ВАЖНО: docs/.vitepress/config.ts должен читать base из env DOCS_BASE
# (см. README-инструкцию), иначе доки соберутся с '/consensus-landscape/docs/'
# и на поддомене отдадут 404 на ассеты.

# ---------- build ----------
# node:22-slim (Debian/glibc), НЕ alpine/musl: frontend/package.json пинит
# @rollup/rollup-linux-x64-gnu (glibc-вариант) — на musl npm ci даёт EBADPLATFORM.
FROM node:22-slim AS build
WORKDIR /app

# Симулятор (SPA). base = '/' вне GITHUB_ACTIONS (см. frontend/vite.config.ts).
COPY frontend/package.json frontend/package-lock.json ./frontend/
RUN cd frontend && npm ci
COPY frontend/ ./frontend/
RUN cd frontend && npm run build

# Документация (VitePress). base = '/docs/' через DOCS_BASE.
COPY package.json package-lock.json ./
RUN npm ci
COPY docs/ ./docs/
ENV DOCS_BASE=/docs/
RUN npm run docs:build

# Слить доки в дерево SPA под /docs.
RUN cp -r docs/.vitepress/dist frontend/dist/docs

# ---------- serve ----------
FROM nginx:1.27-alpine AS serve
COPY deploy/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/frontend/dist /usr/share/nginx/html
EXPOSE 8080
