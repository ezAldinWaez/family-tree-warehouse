#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

docker compose up -d --build

echo "Waiting for Neo4j to be reachable..."
for i in {1..30}; do
  if docker exec family_tree_neo4j cypher-shell -u neo4j -p neo4j_password "RETURN 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Resetting Neo4j graph data..."
docker exec family_tree_neo4j cypher-shell -u neo4j -p neo4j_password "MATCH (n) DETACH DELETE n"

docker exec -i family_tree_neo4j cypher-shell -u neo4j -p neo4j_password < demo_family_tree/init_cruz_young_family_tree.cyp

echo "Waiting for Airflow DAG registration..."
for i in {1..60}; do
  if docker exec family_tree_airflow airflow dags list 2>/dev/null | grep -q "family_tree_dw_incremental"; then
    break
  fi
  sleep 2
done

if ! docker exec family_tree_airflow airflow dags list 2>/dev/null | grep -q "family_tree_dw_incremental"; then
  echo "DAG family_tree_dw_incremental was not registered in time"
  exit 1
fi

docker exec family_tree_airflow airflow dags unpause family_tree_dw_incremental
docker exec family_tree_airflow airflow dags trigger family_tree_dw_incremental

echo "Demo run triggered. Inspect Airflow at http://localhost:8080 and DuckDB at warehouse/family_tree.duckdb"
