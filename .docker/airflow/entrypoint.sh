#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

if ! airflow users list | grep -q "${AIRFLOW_ADMIN_USERNAME}"; then
  airflow users create \
    --role Admin \
    --username "${AIRFLOW_ADMIN_USERNAME}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname Admin \
    --lastname User \
    --email "${AIRFLOW_ADMIN_EMAIL}"
fi

if airflow connections get duckdb_default >/dev/null 2>&1; then
  airflow connections delete duckdb_default
fi

airflow connections add duckdb_default \
  --conn-type duckdb \
  --conn-host "/opt/data/warehouse/warehouse.duckdb"

exec airflow standalone
