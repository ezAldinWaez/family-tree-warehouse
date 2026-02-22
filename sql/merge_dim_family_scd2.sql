CREATE TEMP TABLE tmp_family_latest AS
SELECT
  f.family_id,
  f.marriage_date,
  f.divorce_date,
  f.status,
  md5(
    coalesce(CAST(f.marriage_date AS VARCHAR), '') || '|' ||
    coalesce(CAST(f.divorce_date AS VARCHAR), '') || '|' ||
    coalesce(f.status, '')
  ) AS attr_hash
FROM raw_family_snapshot f
WHERE f.run_id = (SELECT run_id FROM _etl_context LIMIT 1);

CREATE TEMP TABLE tmp_family_changed AS
SELECT d.family_sk
FROM dim_family d
JOIN tmp_family_latest s
  ON d.family_id = s.family_id
WHERE d.is_current = TRUE
  AND d.attr_hash IS DISTINCT FROM s.attr_hash;

UPDATE dim_family
SET is_current = FALSE,
    valid_to_run_id = (SELECT run_id FROM _etl_context LIMIT 1)
WHERE family_sk IN (SELECT family_sk FROM tmp_family_changed);

INSERT INTO dim_family (
  family_id,
  marriage_date,
  divorce_date,
  status,
  attr_hash,
  valid_from_run_id,
  is_current,
  is_deleted
)
SELECT
  s.family_id,
  s.marriage_date,
  s.divorce_date,
  s.status,
  s.attr_hash,
  (SELECT run_id FROM _etl_context LIMIT 1),
  TRUE,
  FALSE
FROM tmp_family_latest s
LEFT JOIN dim_family d
  ON d.family_id = s.family_id
 AND d.is_current = TRUE
WHERE d.family_id IS NULL
   OR d.attr_hash IS DISTINCT FROM s.attr_hash;

UPDATE dim_family d
SET is_current = FALSE,
    is_deleted = TRUE,
    valid_to_run_id = (SELECT run_id FROM _etl_context LIMIT 1)
WHERE d.is_current = TRUE
  AND NOT EXISTS (
    SELECT 1
    FROM raw_family_snapshot f
    WHERE f.run_id = (SELECT run_id FROM _etl_context LIMIT 1)
      AND f.family_id = d.family_id
  );

DROP TABLE tmp_family_changed;
DROP TABLE tmp_family_latest;

