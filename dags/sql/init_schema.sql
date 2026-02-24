-- Create ETL run log table to track execution history
CREATE TABLE IF NOT EXISTS etl_run_log (
  run_id VARCHAR PRIMARY KEY,
  started_at TIMESTAMP,
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

-- Create sequences for surrogate keys
CREATE SEQUENCE IF NOT EXISTS seq_person_sk START 1;
CREATE SEQUENCE IF NOT EXISTS seq_relationship_role_sk START 1;
CREATE SEQUENCE IF NOT EXISTS seq_fact_person_relationship_sk START 1;

-- Create person dimension table with SCD Type 2 tracking
CREATE TABLE IF NOT EXISTS dim_person (
  person_sk BIGINT PRIMARY KEY DEFAULT nextval('seq_person_sk'),
  person_id VARCHAR NOT NULL,
  first_name VARCHAR,
  last_name VARCHAR,
  sex VARCHAR,
  birth_date DATE,
  death_date DATE,
  education VARCHAR,
  works VARCHAR,
  attr_hash VARCHAR,
  valid_from_run_id VARCHAR NOT NULL,
  valid_to_run_id VARCHAR,
  is_current BOOLEAN NOT NULL DEFAULT TRUE,
  is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create relationship role dimension table
CREATE TABLE IF NOT EXISTS dim_relationship_role (
  relationship_role_sk BIGINT PRIMARY KEY DEFAULT nextval('seq_relationship_role_sk'),
  relationship_role_code VARCHAR UNIQUE NOT NULL,
  relationship_type VARCHAR NOT NULL,
  person_role VARCHAR NOT NULL,
  is_symmetric BOOLEAN NOT NULL DEFAULT FALSE,
  inverse_role_code VARCHAR
);

-- Create date dimension table
CREATE TABLE IF NOT EXISTS dim_date (
  date_sk INTEGER PRIMARY KEY,
  full_date DATE UNIQUE NOT NULL,
  day_of_month TINYINT NOT NULL,
  month_of_year TINYINT NOT NULL,
  month_name VARCHAR NOT NULL,
  quarter_of_year TINYINT NOT NULL,
  year_num SMALLINT NOT NULL,
  day_of_week TINYINT NOT NULL,
  day_name VARCHAR NOT NULL,
  is_weekend BOOLEAN NOT NULL
);

-- Create fact table for person relationships
CREATE TABLE IF NOT EXISTS fact_person_relationship (
  relationship_sk BIGINT PRIMARY KEY DEFAULT nextval('seq_fact_person_relationship_sk'),
  source_person_sk BIGINT NOT NULL,
  target_person_sk BIGINT NOT NULL,
  relationship_role_sk BIGINT NOT NULL,
  start_date_sk INTEGER,
  end_date_sk INTEGER,
  source_person_id VARCHAR NOT NULL,
  target_person_id VARCHAR NOT NULL,
  source_relationship_role_code VARCHAR NOT NULL,
  start_date DATE,
  end_date DATE,
  end_reason VARCHAR,
  first_seen_run_id VARCHAR NOT NULL,
  last_seen_run_id VARCHAR NOT NULL,
  is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
  deleted_at_run_id VARCHAR,
  relation_count INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
