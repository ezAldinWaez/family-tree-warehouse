from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from neo4j import GraphDatabase


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


def extract_all(neo4j_uri: str, neo4j_user: str, neo4j_password: str) -> tuple[list[PersonRow], list[FamilyRow], list[RelationRow]]:
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    with driver.session() as session:
        people = [
            _as_person(record.data())
            for record in session.run(PERSON_QUERY)
        ]
        families = [
            _as_family(record.data())
            for record in session.run(FAMILY_QUERY)
        ]
        relations = [
            _as_relation(record.data())
            for record in session.run(RELATION_QUERY)
        ]
    driver.close()
    return people, families, relations
