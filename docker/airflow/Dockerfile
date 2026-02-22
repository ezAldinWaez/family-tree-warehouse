FROM apache/airflow:2.10.5

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
USER airflow

WORKDIR /opt/project
COPY --chown=airflow:root pyproject.toml /opt/project/pyproject.toml
COPY --chown=airflow:root src /opt/project/src

RUN pip install --no-cache-dir duckdb neo4j python-dotenv && \
    pip install --no-cache-dir -e /opt/project
