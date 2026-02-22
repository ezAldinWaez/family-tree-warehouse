from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator

from py.extraction_utils import extract_and_load_raw_snapshot, get_settings


def run_extraction_task() -> str:
    settings = get_settings()
    run_id = extract_and_load_raw_snapshot(
        neo4j_uri=settings.neo4j_uri,
        neo4j_user=settings.neo4j_user,
        neo4j_password=settings.neo4j_password,
        duckdb_path=settings.duckdb_path,
    )
    print(f"Raw extraction complete: {run_id}")
    return run_id


def run_sql_file_task(sql_path: str) -> None:
    settings = get_settings()
    full_sql_path = Path("/opt/airflow/dags") / sql_path
    sql = full_sql_path.read_text(encoding="utf-8")

    conn = duckdb.connect(settings.duckdb_path)
    conn.execute(sql)
    conn.close()
    print(f"Executed SQL file: {full_sql_path}")


with DAG(
    dag_id="family_tree_dw_incremental",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/dags"],
    tags=["etl"],
) as dag:
    init_schema = PythonOperator(
        task_id="init_schema",
        python_callable=run_sql_file_task,
        op_kwargs={"sql_path": "sql/init_schema.sql"},
    )

    extract_raw_data = PythonOperator(
        task_id="extract_raw_data",
        python_callable=run_extraction_task,
    )

    seed_reference_dimensions = PythonOperator(
        task_id="seed_reference_dimensions",
        python_callable=run_sql_file_task,
        op_kwargs={"sql_path": "sql/seed_reference_dimensions.sql"},
    )

    merge_dim_person = PythonOperator(
        task_id="merge_dim_person",
        python_callable=run_sql_file_task,
        op_kwargs={"sql_path": "sql/merge_dim_person_scd2.sql"},
    )

    merge_dim_family = PythonOperator(
        task_id="merge_dim_family",
        python_callable=run_sql_file_task,
        op_kwargs={"sql_path": "sql/merge_dim_family_scd2.sql"},
    )

    merge_fact_relation = PythonOperator(
        task_id="merge_fact_relation",
        python_callable=run_sql_file_task,
        op_kwargs={"sql_path": "sql/merge_fact_relation.sql"},
    )

    init_schema >> extract_raw_data >> seed_reference_dimensions
    seed_reference_dimensions >> [merge_dim_person, merge_dim_family]
    [merge_dim_person, merge_dim_family] >> merge_fact_relation
