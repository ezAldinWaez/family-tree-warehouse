from __future__ import annotations

from datetime import datetime
import os
from uuid import uuid4

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from extraction_utils import extract_and_load_raw_snapshot


def run_extraction_task() -> str:
    run_id = str(uuid4())
    extract_and_load_raw_snapshot(
        neo4j_uri=os.environ.get("NEO4J_URI"),
        neo4j_user=os.environ.get("NEO4J_USER"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD"),
        duckdb_path="/opt/data/warehouse/warehouse.duckdb",
        run_id=run_id,
    )
    print(f"Raw extraction complete: {run_id}")
    return run_id


with DAG(
    dag_id="family_tree_dw_incremental",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/dags"],
    tags=["etl"],
) as dag:
    init_raw_tables = SQLExecuteQueryOperator(
        task_id="init_raw_tables",
        conn_id="duckdb_default",
        sql="sql/init_raw_tables.sql",
        split_statements=True,
    )

    extract_raw_data = PythonOperator(
        task_id="extract_raw_data",
        python_callable=run_extraction_task,
        do_xcom_push=True,
    )

    seed_reference_dimensions = SQLExecuteQueryOperator(
        task_id="seed_dim_relationship_role",
        conn_id="duckdb_default",
        sql="sql/seed_dim_relationship_role.sql",
        split_statements=True,
    )

    merge_dim_person = SQLExecuteQueryOperator(
        task_id="merge_dim_person",
        conn_id="duckdb_default",
        sql="sql/merge_dim_person.sql",
        split_statements=True,
    )

    seed_dim_date = SQLExecuteQueryOperator(
        task_id="seed_dim_date",
        conn_id="duckdb_default",
        sql="sql/seed_dim_date.sql",
        split_statements=True,
    )

    merge_fact_relation = SQLExecuteQueryOperator(
        task_id="merge_fact_relation",
        conn_id="duckdb_default",
        sql="sql/merge_fact_relation.sql",
        split_statements=True,
    )

    init_raw_tables >> extract_raw_data >> [
        seed_reference_dimensions, seed_dim_date, merge_dim_person
    ] >> merge_fact_relation
