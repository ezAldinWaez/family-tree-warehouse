from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from py.extraction_utils import extract_and_load_raw_snapshot, get_settings


def run_extraction_task() -> str:
    settings = get_settings()
    run_id = str(uuid4())
    extract_and_load_raw_snapshot(
        neo4j_uri=settings.neo4j_uri,
        neo4j_user=settings.neo4j_user,
        neo4j_password=settings.neo4j_password,
        duckdb_path=settings.duckdb_path,
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
    init_schema = SQLExecuteQueryOperator(
        task_id="init_schema",
        conn_id="duckdb_default",
        sql="sql/init_schema.sql",
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

    merge_dim_date = SQLExecuteQueryOperator(
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

    init_schema >> extract_raw_data >> seed_reference_dimensions
    seed_reference_dimensions >> [merge_dim_person, merge_dim_date]
    [merge_dim_person, merge_dim_date] >> merge_fact_relation
