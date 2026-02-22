from __future__ import annotations

import argparse

from family_tree_dw.config import get_settings
from family_tree_dw.etl import init_schema, run_all, run_dq_checks, run_extract_and_load, run_merges


def main() -> None:
    parser = argparse.ArgumentParser(description="Family tree incremental ETL")
    parser.add_argument(
        "command",
        choices=["init", "extract-load", "merge", "dq", "run-all"],
        help="Command to run",
    )
    parser.add_argument("--run-id", required=False, help="Optional run id for reproducibility")
    parser.add_argument("--env-file", required=False, help="Path to .env file")

    args = parser.parse_args()
    settings = get_settings(args.env_file)

    if args.command == "init":
        init_schema(settings.duckdb_path)
        print("Schema initialized")
        return

    if args.command == "extract-load":
        run_id = run_extract_and_load(
            neo4j_uri=settings.neo4j_uri,
            neo4j_user=settings.neo4j_user,
            neo4j_password=settings.neo4j_password,
            duckdb_path=settings.duckdb_path,
            run_id=args.run_id,
        )
        print(f"Extract/load complete: run_id={run_id}")
        return

    if args.command == "merge":
        if not args.run_id:
            raise ValueError("--run-id is required for merge")
        run_merges(settings.duckdb_path, args.run_id)
        print(f"Merge complete: run_id={args.run_id}")
        return

    if args.command == "dq":
        if not args.run_id:
            raise ValueError("--run-id is required for dq")
        run_dq_checks(settings.duckdb_path, args.run_id)
        print(f"DQ checks passed: run_id={args.run_id}")
        return

    run_id = run_all(
        neo4j_uri=settings.neo4j_uri,
        neo4j_user=settings.neo4j_user,
        neo4j_password=settings.neo4j_password,
        duckdb_path=settings.duckdb_path,
        run_id=args.run_id,
    )
    print(f"ETL completed: run_id={run_id}")


if __name__ == "__main__":
    main()
