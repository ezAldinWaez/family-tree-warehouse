"""Neo4j extraction utilities for Airflow DAG."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
from dotenv import load_dotenv
from neo4j import GraphDatabase


# ============================================================================
# Configuration
# ============================================================================


@dataclass(frozen=True)
class Settings:
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    duckdb_path: str


def get_settings(dotenv_path: str | None = None) -> Settings:
    load_dotenv(dotenv_path)
    duckdb_path = "/opt/warehouse/family_tree.duckdb"
    return Settings(
        neo4j_uri=os.environ.get("NEO4J_URI"),
        neo4j_user=os.environ.get("NEO4J_USER"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD"),
        duckdb_path=duckdb_path,
    )


# ============================================================================
# DuckDB helpers
# ============================================================================


def ensure_parent_dir(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)


def connect_duckdb(db_path: str) -> duckdb.DuckDBPyConnection:
    ensure_parent_dir(db_path)
    return duckdb.connect(database=db_path)


# ============================================================================
# Neo4j extraction
# ============================================================================


@dataclass(frozen=True)
class PersonRow:
    person_id: str
    first_name: str
    last_name: str
    sex: str
    birth_date: date
    death_date: date | None
    education: str
    works: str


@dataclass(frozen=True)
class FamilyRow:
    family_id: str
    marriage_date: date
    divorce_date: date | None
    status: str


@dataclass(frozen=True)
class RelationRow:
    person_id: str
    family_id: str
    relationship_type: str
    role: str


PERSON_QUERY = """
MATCH (p:Person)
RETURN
  p.id AS person_id,
  p.first_name AS first_name,
  p.last_name AS last_name,
  p.sex AS sex,
  p.birth_date AS birth_date,
  p.death_date AS death_date,
  p.education AS education,
  p.works AS works
"""

FAMILY_QUERY = """
MATCH (f:Family)
RETURN
  f.id AS family_id,
  f.marriage_date AS marriage_date,
  f.divorce_date AS divorce_date,
  f.status AS status
"""

RELATION_QUERY = """
MATCH (p:Person)-[r:SPOUSE_OF|CHILD_OF]->(f:Family)
RETURN
  p.id AS person_id,
  f.id AS family_id,
  type(r) AS relationship_type,
  coalesce(r.role, 'NONE') AS role
"""


def _normalize_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if hasattr(value, "to_native"):
        native_value = value.to_native()
        if isinstance(native_value, date):
            return native_value
    raise TypeError(f"Unsupported date value type: {type(value)}")


def _as_person(row: dict) -> PersonRow:
    return PersonRow(
        person_id=row["person_id"],
        first_name=row["first_name"],
        last_name=row["last_name"],
        sex=row["sex"],
        birth_date=_normalize_date(row["birth_date"]),
        death_date=_normalize_date(row["death_date"]),
        education=row["education"],
        works=row["works"],
    )


def _as_family(row: dict) -> FamilyRow:
    return FamilyRow(
        family_id=row["family_id"],
        marriage_date=_normalize_date(row["marriage_date"]),
        divorce_date=_normalize_date(row["divorce_date"]),
        status=row["status"],
    )


def _as_relation(row: dict) -> RelationRow:
    return RelationRow(
        person_id=row["person_id"],
        family_id=row["family_id"],
        relationship_type=row["relationship_type"],
        role=row["role"],
    )


def extract_all(
    neo4j_uri: str, neo4j_user: str, neo4j_password: str
) -> tuple[list[PersonRow], list[FamilyRow], list[RelationRow]]:
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    with driver.session() as session:
        people = [_as_person(record.data()) for record in session.run(PERSON_QUERY)]
        families = [_as_family(record.data()) for record in session.run(FAMILY_QUERY)]
        relations = [
            _as_relation(record.data()) for record in session.run(RELATION_QUERY)
        ]
    driver.close()
    return people, families, relations


# ============================================================================
# Extraction + Load (for Airflow task)
# ============================================================================


def extract_and_load_raw_snapshot(
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    duckdb_path: str,
    run_id: str | None = None,
) -> str:
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

    conn.execute(
        """
        INSERT INTO etl_run_log(run_id, started_at, status, dq_passed)
        VALUES (?, ?, 'RUNNING', FALSE)
        """,
        [resolved_run_id, extracted_at],
    )
    conn.close()
    return resolved_run_id
