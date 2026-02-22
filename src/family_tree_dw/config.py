from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    duckdb_path: str


def get_settings(dotenv_path: str | None = None) -> Settings:
    load_dotenv(dotenv_path)
    return Settings(
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", "neo4j"),
        duckdb_path=os.environ.get("DUCKDB_PATH", "warehouse/family_tree.duckdb"),
    )
