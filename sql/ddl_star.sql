CREATE TABLE IF NOT EXISTS etl_run_log (
  run_id VARCHAR PRIMARY KEY,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  status VARCHAR,
  dq_passed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS raw_person_snapshot (
  run_id VARCHAR,
  extracted_at TIMESTAMP,
  person_id VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  sex VARCHAR,
  birth_date DATE,
  death_date DATE,
  education VARCHAR,
  works VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_family_snapshot (
  run_id VARCHAR,
  extracted_at TIMESTAMP,
  family_id VARCHAR,
  marriage_date DATE,
  divorce_date DATE,
  status VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_relation_snapshot (
  run_id VARCHAR,
  extracted_at TIMESTAMP,
  person_id VARCHAR,
  family_id VARCHAR,
  relationship_type VARCHAR,
  role VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS seq_person_sk START 1;
CREATE SEQUENCE IF NOT EXISTS seq_family_sk START 1;
CREATE SEQUENCE IF NOT EXISTS seq_rel_type_sk START 1;
CREATE SEQUENCE IF NOT EXISTS seq_role_sk START 1;
CREATE SEQUENCE IF NOT EXISTS seq_fact_rel_sk START 1;

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

CREATE TABLE IF NOT EXISTS dim_family (
  family_sk BIGINT PRIMARY KEY DEFAULT nextval('seq_family_sk'),
  family_id VARCHAR NOT NULL,
  marriage_date DATE,
  divorce_date DATE,
  status VARCHAR,
  attr_hash VARCHAR,
  valid_from_run_id VARCHAR NOT NULL,
  valid_to_run_id VARCHAR,
  is_current BOOLEAN NOT NULL DEFAULT TRUE,
  is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_relationship_type (
  relationship_type_sk BIGINT PRIMARY KEY DEFAULT nextval('seq_rel_type_sk'),
  relationship_code VARCHAR UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_role (
  role_sk BIGINT PRIMARY KEY DEFAULT nextval('seq_role_sk'),
  role_code VARCHAR UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_person_family_relation (
  relation_sk BIGINT PRIMARY KEY DEFAULT nextval('seq_fact_rel_sk'),
  person_sk BIGINT NOT NULL,
  family_sk BIGINT NOT NULL,
  relationship_type_sk BIGINT NOT NULL,
  role_sk BIGINT NOT NULL,
  source_person_id VARCHAR NOT NULL,
  source_family_id VARCHAR NOT NULL,
  source_relationship_type VARCHAR NOT NULL,
  source_role VARCHAR NOT NULL,
  first_seen_run_id VARCHAR NOT NULL,
  last_seen_run_id VARCHAR NOT NULL,
  is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
  deleted_at_run_id VARCHAR,
  relation_count INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE (source_person_id, source_family_id, source_relationship_type, source_role)
);
