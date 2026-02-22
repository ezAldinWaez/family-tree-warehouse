from __future__ import annotations

from pathlib import Path

import duckdb


def ensure_parent_dir(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)


def connect_duckdb(db_path: str) -> duckdb.DuckDBPyConnection:
    ensure_parent_dir(db_path)
    return duckdb.connect(database=db_path)


def run_sql_file(conn: duckdb.DuckDBPyConnection, sql_file: str) -> None:
    sql_text = Path(sql_file).read_text(encoding="utf-8")
    conn.execute(sql_text)
