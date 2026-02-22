from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import uuid4

from family_tree_dw.db import connect_duckdb, run_sql_file
from family_tree_dw.extract import extract_all


def _sql_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "sql"


def init_schema(duckdb_path: str) -> None:
    conn = connect_duckdb(duckdb_path)
    run_sql_file(conn, str(_sql_dir() / "ddl_star.sql"))
    conn.close()


def run_extract_and_load(neo4j_uri: str, neo4j_user: str, neo4j_password: str, duckdb_path: str, run_id: str | None = None) -> str:
    people, families, relations = extract_all(neo4j_uri, neo4j_user, neo4j_password)
    resolved_run_id = run_id or str(uuid4())
    extracted_at = datetime.utcnow()

    conn = connect_duckdb(duckdb_path)

    for person in people:
        conn.execute(
            """
            INSERT INTO raw_person_snapshot (
              run_id, extracted_at, person_id, first_name, last_name, sex,
              birth_date, death_date, education, works
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                resolved_run_id,
                extracted_at,
                person.person_id,
                person.first_name,
                person.last_name,
                person.sex,
                person.birth_date,
                person.death_date,
                person.education,
                person.works,
            ],
        )

    for family in families:
        conn.execute(
            """
            INSERT INTO raw_family_snapshot (
              run_id, extracted_at, family_id, marriage_date, divorce_date, status
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                resolved_run_id,
                extracted_at,
                family.family_id,
                family.marriage_date,
                family.divorce_date,
                family.status,
            ],
        )

    for relation in relations:
        conn.execute(
            """
            INSERT INTO raw_relation_snapshot (
              run_id, extracted_at, person_id, family_id, relationship_type, role
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                resolved_run_id,
                extracted_at,
                relation.person_id,
                relation.family_id,
                relation.relationship_type,
                relation.role,
            ],
        )

    conn.execute("INSERT INTO etl_run_log(run_id, started_at, completed_at, status) VALUES (?, ?, ?, 'SUCCESS')", [resolved_run_id, extracted_at, datetime.utcnow()])
    conn.close()
    return resolved_run_id


def run_merges(duckdb_path: str, run_id: str) -> None:
    conn = connect_duckdb(duckdb_path)
    conn.execute(
        """
        INSERT INTO dim_relationship_type(relationship_code)
        VALUES ('SPOUSE_OF'), ('CHILD_OF')
        ON CONFLICT DO NOTHING
        """
    )
    conn.execute(
        """
        INSERT INTO dim_role(role_code)
        VALUES ('HUSBAND'), ('WIFE'), ('NONE')
        ON CONFLICT DO NOTHING
        """
    )
    conn.execute("CREATE TEMP TABLE _etl_context (run_id VARCHAR)")
    conn.execute("INSERT INTO _etl_context VALUES (?)", [run_id])
    run_sql_file(conn, str(_sql_dir() / "merge_dim_person_scd2.sql"))
    run_sql_file(conn, str(_sql_dir() / "merge_dim_family_scd2.sql"))
    run_sql_file(conn, str(_sql_dir() / "merge_fact_relation.sql"))
    conn.execute("DROP TABLE _etl_context")
    conn.close()


def run_dq_checks(duckdb_path: str, run_id: str) -> None:
    conn = connect_duckdb(duckdb_path)

    dup_person_keys = conn.execute(
        """
        SELECT person_id, COUNT(*)
        FROM dim_person
        WHERE is_current = TRUE
        GROUP BY person_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    if dup_person_keys:
        raise ValueError(f"DQ failed: multiple current person rows found: {dup_person_keys}")

    dup_family_keys = conn.execute(
        """
        SELECT family_id, COUNT(*)
        FROM dim_family
        WHERE is_current = TRUE
        GROUP BY family_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    if dup_family_keys:
        raise ValueError(f"DQ failed: multiple current family rows found: {dup_family_keys}")

    bad_status = conn.execute(
        """
        SELECT DISTINCT status
        FROM dim_family
        WHERE is_current = TRUE
          AND status NOT IN ('ACTIVE', 'DIVORCED', 'WIDOWED')
        """
    ).fetchall()
    if bad_status:
        raise ValueError(f"DQ failed: unexpected family status values: {bad_status}")

    bad_role = conn.execute(
        """
        SELECT DISTINCT role_code
        FROM dim_role
        WHERE role_code NOT IN ('HUSBAND', 'WIFE', 'NONE')
        """
    ).fetchall()
    if bad_role:
        raise ValueError(f"DQ failed: unexpected role codes in dim_role: {bad_role}")

    fact_null_fks = conn.execute(
        """
        SELECT COUNT(*)
        FROM fact_person_family_relation
        WHERE person_sk IS NULL OR family_sk IS NULL OR relationship_type_sk IS NULL OR role_sk IS NULL
        """
    ).fetchone()[0]
    if fact_null_fks > 0:
        raise ValueError("DQ failed: null foreign keys found in fact_person_family_relation")

    conn.execute("UPDATE etl_run_log SET dq_passed = TRUE WHERE run_id = ?", [run_id])
    conn.close()


def run_all(neo4j_uri: str, neo4j_user: str, neo4j_password: str, duckdb_path: str, run_id: str | None = None) -> str:
    init_schema(duckdb_path)
    resolved_run_id = run_extract_and_load(
        neo4j_uri=neo4j_uri,
        neo4j_user=neo4j_user,
        neo4j_password=neo4j_password,
        duckdb_path=duckdb_path,
        run_id=run_id,
    )
    run_merges(duckdb_path=duckdb_path, run_id=resolved_run_id)
    run_dq_checks(duckdb_path=duckdb_path, run_id=resolved_run_id)
    return resolved_run_id
