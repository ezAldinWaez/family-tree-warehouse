-- Create ETL run log table to track execution history
CREATE TABLE IF NOT EXISTS etl_run_log (
  run_id VARCHAR PRIMARY KEY,
  started_at TIMESTAMP
);

-- Create raw person snapshot table to store source data
CREATE TABLE IF NOT EXISTS raw_person_snapshot (
  run_id VARCHAR,
  person_id VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  sex VARCHAR,
  birth_date DATE,
  death_date DATE,
  education VARCHAR,
  works VARCHAR
);

-- Create raw family snapshot table to store source data
CREATE TABLE IF NOT EXISTS raw_family_snapshot (
  run_id VARCHAR,
  family_id VARCHAR,
  marriage_date DATE,
  divorce_date DATE,
  status VARCHAR
);

-- Create raw relation snapshot table to store source data
CREATE TABLE IF NOT EXISTS raw_relation_snapshot (
  run_id VARCHAR,
  person_id VARCHAR,
  family_id VARCHAR,
  relationship_type VARCHAR,
  role VARCHAR
);
