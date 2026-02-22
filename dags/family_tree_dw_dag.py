from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from family_tree_dw.config import get_settings
from family_tree_dw.etl import run_all


def run_pipeline() -> None:
    settings = get_settings()
    run_id = run_all(
        neo4j_uri=settings.neo4j_uri,
        neo4j_user=settings.neo4j_user,
        neo4j_password=settings.neo4j_password,
        duckdb_path=settings.duckdb_path,
    )
    print(f"Pipeline run completed: {run_id}")


with DAG(
    dag_id="family_tree_dw_incremental",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["neo4j", "duckdb", "dw", "star-schema"],
) as dag:
    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=run_pipeline,
    )

    run_etl
