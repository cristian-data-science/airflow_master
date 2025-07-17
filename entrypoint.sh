#!/usr/bin/env bash
set -e

# 1) Migraciones
airflow db upgrade

# 2) Usuario admin (idempotente)
airflow users create \
  --username "$AIRFLOW_ADMIN_USER" \
  --firstname "Admin" \
  --lastname "User" \
  --role "Admin" \
  --password "$AIRFLOW_ADMIN_PASSWORD" \
  --email "admin@example.com" || true

# 3) Scheduler SIN mini-servidor de logs (background)
airflow scheduler --skip-serve-logs &

# 4) Webserver en primer plano (mantiene el contenedor vivo)
exec airflow webserver
