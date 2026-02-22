WITH current_run AS (
  SELECT '{{ ti.xcom_pull(task_ids="extract_raw_data") }}' AS run_id
), latest_rel AS (
  SELECT
    r.person_id,
    r.family_id,
    r.relationship_type,
    r.role
  FROM raw_relation_snapshot r
  JOIN current_run c
    ON r.run_id = c.run_id
), with_keys AS (
  SELECT
    p.person_sk,
    f.family_sk,
    t.relationship_type_sk,
    ro.role_sk,
    lr.person_id,
    lr.family_id,
    lr.relationship_type,
    lr.role
  FROM latest_rel lr
  JOIN dim_person p
    ON p.person_id = lr.person_id
   AND p.is_current = TRUE
  JOIN dim_family f
    ON f.family_id = lr.family_id
   AND f.is_current = TRUE
  JOIN dim_relationship_type t
    ON t.relationship_code = lr.relationship_type
  JOIN dim_role ro
    ON ro.role_code = lr.role
)
INSERT INTO fact_person_family_relation (
  person_sk,
  family_sk,
  relationship_type_sk,
  role_sk,
  source_person_id,
  source_family_id,
  source_relationship_type,
  source_role,
  first_seen_run_id,
  last_seen_run_id,
  is_deleted,
  relation_count,
  updated_at
)
SELECT
  wk.person_sk,
  wk.family_sk,
  wk.relationship_type_sk,
  wk.role_sk,
  wk.person_id,
  wk.family_id,
  wk.relationship_type,
  wk.role,
  (SELECT run_id FROM current_run),
  (SELECT run_id FROM current_run),
  FALSE,
  1,
  NOW()
FROM with_keys wk
LEFT JOIN fact_person_family_relation fct
  ON fct.source_person_id = wk.person_id
 AND fct.source_family_id = wk.family_id
 AND fct.source_relationship_type = wk.relationship_type
 AND fct.source_role = wk.role
WHERE fct.relation_sk IS NULL;

WITH current_run AS (
  SELECT '{{ ti.xcom_pull(task_ids="extract_raw_data") }}' AS run_id
), latest_rel AS (
  SELECT
    person_id,
    family_id,
    relationship_type,
    role
  FROM raw_relation_snapshot
  WHERE run_id = (SELECT run_id FROM current_run)
)
UPDATE fact_person_family_relation fct
SET person_sk = p.person_sk,
    family_sk = f.family_sk,
    relationship_type_sk = t.relationship_type_sk,
    role_sk = ro.role_sk,
    last_seen_run_id = (SELECT run_id FROM current_run),
    is_deleted = FALSE,
    deleted_at_run_id = NULL,
    updated_at = NOW()
FROM dim_person p, dim_family f, dim_relationship_type t, dim_role ro, latest_rel lr
WHERE fct.source_person_id = lr.person_id
  AND fct.source_family_id = lr.family_id
  AND fct.source_relationship_type = lr.relationship_type
  AND fct.source_role = lr.role
  AND p.person_id = lr.person_id AND p.is_current = TRUE
  AND f.family_id = lr.family_id AND f.is_current = TRUE
  AND t.relationship_code = lr.relationship_type
  AND ro.role_code = lr.role;

WITH current_run AS (
  SELECT '{{ ti.xcom_pull(task_ids="extract_raw_data") }}' AS run_id
), latest_rel AS (
  SELECT
    person_id,
    family_id,
    relationship_type,
    role
  FROM raw_relation_snapshot
  WHERE run_id = (SELECT run_id FROM current_run)
)
UPDATE fact_person_family_relation fct
SET is_deleted = TRUE,
    deleted_at_run_id = (SELECT run_id FROM current_run),
    updated_at = NOW()
WHERE NOT EXISTS (
  SELECT 1
  FROM latest_rel lr
  WHERE lr.person_id = fct.source_person_id
    AND lr.family_id = fct.source_family_id
    AND lr.relationship_type = fct.source_relationship_type
    AND lr.role = fct.source_role
)
  AND fct.is_deleted = FALSE;
