#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

airflow scheduler &
exec airflow webserver
