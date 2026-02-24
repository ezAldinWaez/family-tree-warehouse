"""Neo4j extraction utilities for Airflow DAG."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from dotenv import load_dotenv
from neo4j import GraphDatabase


@dataclass(frozen=True)
class Settings:
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    duckdb_path: str


def connect_duckdb(db_path: str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(database=db_path)


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
RETURN p { .* } AS person_data
"""

FAMILY_QUERY = """
MATCH (f:Family)
RETURN f { .* } AS family_data
"""

RELATION_QUERY = """
MATCH (p:Person)-[r]->(f:Family)
RETURN
  p.id AS person_id,
  f.id AS family_id,
  type(r) AS relationship_type,
  r {.*} AS relationship_data
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


def _as_person(record: dict) -> PersonRow:
    assert "person_data" in record and isinstance(record["person_data"], dict)
    return PersonRow(
        person_id=record["person_data"].get("id"),
        first_name=record["person_data"].get("first_name"),
        last_name=record["person_data"].get("last_name"),
        sex=record["person_data"].get("sex"),
        birth_date=_normalize_date(record["person_data"].get("birth_date")),
        death_date=_normalize_date(record["person_data"].get("death_date")),
        education=record["person_data"].get("education"),
        works=record["person_data"].get("works"),
    )


def _as_family(record: dict) -> FamilyRow:
    assert "family_data" in record and isinstance(record["family_data"], dict)
    return FamilyRow(
        family_id=record["family_data"].get("id"),
        marriage_date=_normalize_date(record["family_data"].get("marriage_date")),
        divorce_date=_normalize_date(record["family_data"].get("divorce_date")),
        status=record["family_data"].get("status"),
    )


def _as_relation(record: dict) -> RelationRow:
    assert ("person_id" in record and "family_id" in record and "relationship_type" in record
            and "relationship_data" in record and isinstance(record["relationship_data"], dict))
    return RelationRow(
        person_id=record["person_id"],
        family_id=record["family_id"],
        relationship_type=record["relationship_type"],
        role=record["relationship_data"].get("role"),
    )


def extract_all(
    neo4j_uri: str, neo4j_user: str, neo4j_password: str
) -> tuple[list[PersonRow], list[FamilyRow], list[RelationRow]]:
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    with driver.session() as session:
        people = [_as_person(record.data()) for record in session.run(PERSON_QUERY)]
        families = [_as_family(record.data()) for record in session.run(FAMILY_QUERY)]
        relations = [_as_relation(record.data()) for record in session.run(RELATION_QUERY)]
    driver.close()
    return people, families, relations


def extract_and_load_raw_snapshot(
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    duckdb_path: str,
    run_id: str,
) -> None:
    started_at = datetime.now(timezone.utc)

    people, families, relations = extract_all(neo4j_uri, neo4j_user, neo4j_password)

    conn = connect_duckdb(duckdb_path)

    for person in people:
        conn.execute(
            """
            INSERT INTO raw_person_snapshot (
              run_id, person_id, first_name, last_name, sex,
              birth_date, death_date, education, works
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
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
              run_id, family_id, marriage_date, divorce_date, status
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                run_id,
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
              run_id, person_id, family_id, relationship_type, role
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                run_id,
                relation.person_id,
                relation.family_id,
                relation.relationship_type,
                relation.role,
            ],
        )

    conn.execute(
        """
        INSERT INTO etl_run_log(run_id, started_at)
        VALUES (?, ?)
        """,
        [run_id, started_at],
    )

    conn.close()
