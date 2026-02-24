-- Get the latest ETL run ID
CREATE TEMP TABLE tmp_current_run AS
SELECT run_id
FROM etl_run_log
ORDER BY started_at DESC
LIMIT 1;

-- Extract latest person data and compute hash for change detection
CREATE TEMP TABLE tmp_person_latest AS
SELECT
  p.person_id,
  p.first_name,
  p.last_name,
  p.sex,
  p.birth_date,
  p.death_date,
  p.education,
  p.works,
  md5(
    coalesce(p.first_name, '') || '|' ||
    coalesce(p.last_name, '') || '|' ||
    coalesce(p.sex, '') || '|' ||
    coalesce(CAST(p.birth_date AS VARCHAR), '') || '|' ||
    coalesce(CAST(p.death_date AS VARCHAR), '') || '|' ||
    coalesce(p.education, '') || '|' ||
    coalesce(p.works, '')
  ) AS attr_hash
FROM raw_person_snapshot p
WHERE p.run_id = (SELECT run_id FROM tmp_current_run);

-- Identify persons with changed attributes
CREATE TEMP TABLE tmp_person_changed AS
SELECT d.person_sk
FROM dim_person d
JOIN tmp_person_latest s ON d.person_id = s.person_id
WHERE d.is_current = TRUE AND d.attr_hash IS DISTINCT FROM s.attr_hash;

-- Mark changed records as non-current
UPDATE dim_person
SET is_current = FALSE, valid_to_run_id = (SELECT run_id FROM tmp_current_run)
WHERE person_sk IN (SELECT person_sk FROM tmp_person_changed);

-- Insert new and changed person records
INSERT INTO dim_person (
  person_id,
  first_name,
  last_name,
  sex,
  birth_date,
  death_date,
  education,
  works,
  attr_hash,
  valid_from_run_id,
  is_current,
  is_deleted
)
SELECT
  s.person_id,
  s.first_name,
  s.last_name,
  s.sex,
  s.birth_date,
  s.death_date,
  s.education,
  s.works,
  s.attr_hash,
  (SELECT run_id FROM tmp_current_run),
  TRUE,
  FALSE
FROM tmp_person_latest s
LEFT JOIN dim_person d ON d.person_id = s.person_id AND d.is_current = TRUE
WHERE d.person_id IS NULL OR d.attr_hash IS DISTINCT FROM s.attr_hash;

-- Mark persons as deleted if no longer in source
UPDATE dim_person d
SET is_current = FALSE, is_deleted = TRUE, valid_to_run_id = (SELECT run_id FROM tmp_current_run)
WHERE d.is_current = TRUE AND NOT EXISTS (
  SELECT 1
  FROM raw_person_snapshot p
  WHERE p.run_id = (SELECT run_id FROM tmp_current_run)
    AND p.person_id = d.person_id
);

-- Clean up temporary tables
DROP TABLE tmp_person_changed;
DROP TABLE tmp_person_latest;
DROP TABLE tmp_current_run;
