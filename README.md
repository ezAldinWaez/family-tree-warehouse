# Family Tree Warehouse

Incremental Neo4j → DuckDB ETL with an exact star schema for family graph analytics.

## What this project does

- Extracts `Person`, `Family`, and relationship data (`SPOUSE_OF`, `CHILD_OF`) from Neo4j.
- Stores each extraction as an append-only raw snapshot in DuckDB (`run_id` based).
- Builds star-schema warehouse tables with:
  - SCD2 dimensions for `person` and `family`.
  - Static dimensions for relationship type and role.
  - A unified relationship fact table.
- Runs data-quality checks after each load.

## Architecture flow

```mermaid
flowchart LR
flowchart TD
  A["Neo4j Graph<br>Person, Family, SPOUSE_OF, CHILD_OF"] --> B["Python Extract Layer\nextract.py"]
  B --> C["DuckDB Raw Snapshots<br>raw_person_snapshot<br>raw_family_snapshot<br>raw_relation_snapshot"]
  C --> D["SCD2 Dimension Merges<br>merge_dim_person_scd2.sql<br>merge_dim_family_scd2.sql"]
  C --> E["Fact Merge<br>merge_fact_relation.sql"]
  D --> F["Star Schema in DuckDB<br>dim_person<br>dim_family<br>dim_relationship_type<br>dim_role<br>fact_person_family_relation"]
  E --> F
  F --> G["Data Quality Checks<br>run_dq_checks"]
```

## Star schema design

### Dimensions

- `dim_person` (SCD2): person attributes and versioning columns (`is_current`, `valid_from_run_id`, `valid_to_run_id`, `is_deleted`).
- `dim_family` (SCD2): family attributes and versioning columns.
- `dim_relationship_type`: `SPOUSE_OF`, `CHILD_OF`.
- `dim_role`: `HUSBAND`, `WIFE`, `NONE`.

### Fact

- `fact_person_family_relation` at grain:
  - one row per `(person_id, family_id, relationship_type, role)` key.
- Tracks first/last seen run, soft deletes, and FK links to current dimension rows.

## Incremental strategy

Neo4j source currently has no `updated_at` columns, so this project uses **snapshot-diff incrementals**:

1. Extract source state into raw snapshot tables for a generated `run_id`.
2. Compute SCD2 changes in dimensions using attribute hashes.
3. Upsert fact rows from current snapshot and mark missing keys as soft-deleted.

This gives reliable incrementals without requiring CDC fields in source data.

## Repository structure

- `src/family_tree_dw/`
  - `extract.py`: Neo4j queries and row mapping.
  - `etl.py`: orchestrates schema init, extract/load, merges, and DQ.
  - `cli.py`: command-line interface.
  - `config.py`: environment-driven settings.
  - `db.py`: DuckDB helpers.
- `sql/`
  - `ddl_star.sql`
  - `merge_dim_person_scd2.sql`
  - `merge_dim_family_scd2.sql`
  - `merge_fact_relation.sql`
- `dags/family_tree_dw_dag.py`: Airflow DAG.
- `docker-compose.yml`: local Neo4j + Airflow stack.
- `docker/airflow/`: Airflow image and bootstrap script.
- `scripts/run_demo_e2e.sh`: one-command local demo run.
- `demo_family_tree/init_cruz_young_family_tree.cyp`: sample graph seed.

## Prerequisites

- Python `>= 3.10`
- Docker + Docker Compose (for full local stack)
- Neo4j reachable from your runtime environment

## Configuration

Copy and edit environment file:

```bash
cp configs/settings.example.env .env
```

Required values:

- `NEO4J_URI`
- `NEO4J_USER`
- `NEO4J_PASSWORD`
- `DUCKDB_PATH`

## Local CLI usage (without Airflow)

Install dependencies:

```bash
pip install -r requirements.txt
```

Initialize schema:

```bash
PYTHONPATH=src python -m family_tree_dw.cli init --env-file .env
```

Run full pipeline:

```bash
PYTHONPATH=src python -m family_tree_dw.cli run-all --env-file .env
```

Run step-by-step:

```bash
PYTHONPATH=src python -m family_tree_dw.cli extract-load --env-file .env
PYTHONPATH=src python -m family_tree_dw.cli merge --env-file .env --run-id <RUN_ID>
PYTHONPATH=src python -m family_tree_dw.cli dq --env-file .env --run-id <RUN_ID>
```

## Docker + Airflow quick start

### Option A: one command demo

```bash
bash scripts/run_demo_e2e.sh
```

This script:

1. Builds/starts services.
2. Waits for Neo4j readiness.
3. Resets Neo4j demo graph (`MATCH (n) DETACH DELETE n`).
4. Loads demo Cypher data.
5. Waits for DAG registration.
6. Unpauses and triggers `family_tree_dw_incremental`.

### Option B: manual commands

Start services:

```bash
docker compose up -d --build
```

Seed demo data:

```bash
docker exec -i family_tree_neo4j cypher-shell -u neo4j -p neo4j_password < demo_family_tree/init_cruz_young_family_tree.cyp
```

Trigger DAG:

```bash
docker exec family_tree_airflow airflow dags unpause family_tree_dw_incremental
docker exec family_tree_airflow airflow dags trigger family_tree_dw_incremental
```

Airflow UI:

- URL: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

## Validation queries

Check latest DAG runs:

```bash
docker exec family_tree_airflow airflow dags list-runs -d family_tree_dw_incremental
```

Check warehouse counts:

```bash
docker exec family_tree_airflow python -c "import duckdb; c=duckdb.connect('/opt/project/warehouse/family_tree.duckdb'); tables=['raw_person_snapshot','raw_family_snapshot','raw_relation_snapshot','dim_person','dim_family','dim_relationship_type','dim_role','fact_person_family_relation']; [print(t, c.execute('SELECT COUNT(*) FROM '+t).fetchone()[0]) for t in tables]"
```

## Data quality checks included

- One current row per business key in SCD2 dimensions.
- Allowed domain values for family status and relationship roles.
- Non-null fact foreign keys.

## Troubleshooting

- `DagNotFound`: wait until Airflow parses DAGs (handled in demo script).
- Neo4j seed duplicate key error: reseed on a clean graph or use demo script reset step.
- Docker permission denied: run Docker commands with the required permissions for your host.
- DuckDB temporal conversion errors: this project normalizes Neo4j temporal values to Python native dates in extraction.

## Notes

- This repository ignores generated local artifacts (`__pycache__`, `.duckdb`, `warehouse/`).
- The demo script is designed for reproducible reruns during development.
