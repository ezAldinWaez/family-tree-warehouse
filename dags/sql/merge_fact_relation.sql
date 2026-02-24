-- Get the latest ETL run ID
CREATE TEMP TABLE tmp_current_run AS
SELECT run_id
FROM etl_run_log
ORDER BY started_at DESC
LIMIT 1;

-- Extract latest person birth dates
CREATE TEMP TABLE tmp_person_latest AS
SELECT
  p.person_id,
  p.birth_date
FROM raw_person_snapshot p
WHERE p.run_id = (SELECT run_id FROM tmp_current_run);

-- Extract latest family data
CREATE TEMP TABLE tmp_family_latest AS
SELECT
  f.family_id,
  f.marriage_date,
  f.divorce_date,
  f.status
FROM raw_family_snapshot f
WHERE f.run_id = (SELECT run_id FROM tmp_current_run);

-- Extract latest relation data and normalize case
CREATE TEMP TABLE tmp_relation_latest AS
SELECT
  r.person_id,
  r.family_id,
  upper(r.relationship_type) AS relationship_type,
  upper(coalesce(r.role, 'NONE')) AS role
FROM raw_relation_snapshot r
WHERE r.run_id = (SELECT run_id FROM tmp_current_run);

-- Get spouse relationships with family dates
CREATE TEMP TABLE tmp_family_spouse AS
SELECT
  lr.person_id,
  lr.family_id,
  lr.role,
  lf.marriage_date,
  lf.divorce_date,
  lf.status
FROM tmp_relation_latest lr
JOIN tmp_family_latest lf
  ON lf.family_id = lr.family_id
WHERE lr.relationship_type = 'SPOUSE_OF';

-- Build spouse relationship edges
CREATE TEMP TABLE tmp_spouse_edge AS
SELECT DISTINCT
  s1.person_id AS source_person_id,
  s2.person_id AS target_person_id,
  CASE
    WHEN s1.role = 'HUSBAND' THEN 'SPOUSE_HUSBAND'
    WHEN s1.role = 'WIFE' THEN 'SPOUSE_WIFE'
    ELSE 'SPOUSE_PARTNER'
  END AS relationship_role_code,
  s1.marriage_date AS start_date,
  s1.divorce_date AS end_date,
  CASE
    WHEN s1.divorce_date IS NOT NULL THEN 'DIVORCE'
    WHEN upper(coalesce(s1.status, '')) IN ('DIVORCED', 'ENDED') THEN 'STATUS_ENDED'
    ELSE NULL
  END AS end_reason
FROM tmp_family_spouse s1
JOIN tmp_family_spouse s2
  ON s1.family_id = s2.family_id
 AND s1.person_id <> s2.person_id;

-- Extract parent IDs from family
CREATE TEMP TABLE tmp_family_parent AS
SELECT
  person_id AS parent_id,
  family_id,
  marriage_date
FROM tmp_family_spouse;

-- Extract child relationships with birth dates
CREATE TEMP TABLE tmp_family_child AS
SELECT
  lr.person_id AS child_id,
  lr.family_id,
  lp.birth_date AS child_birth_date
FROM tmp_relation_latest lr
LEFT JOIN tmp_person_latest lp
  ON lp.person_id = lr.person_id
WHERE lr.relationship_type = 'CHILD_OF';

-- Build parent-child relationship edges in both directions
CREATE TEMP TABLE tmp_parent_child_edge AS
SELECT DISTINCT
  fp.parent_id AS source_person_id,
  fc.child_id AS target_person_id,
  'PARENT' AS relationship_role_code,
  coalesce(fp.marriage_date, fc.child_birth_date) AS start_date,
  CAST(NULL AS DATE) AS end_date,
  CAST(NULL AS VARCHAR) AS end_reason
FROM tmp_family_parent fp
JOIN tmp_family_child fc
  ON fp.family_id = fc.family_id

UNION ALL

SELECT DISTINCT
  fc.child_id AS source_person_id,
  fp.parent_id AS target_person_id,
  'CHILD' AS relationship_role_code,
  fc.child_birth_date AS start_date,
  CAST(NULL AS DATE) AS end_date,
  CAST(NULL AS VARCHAR) AS end_reason
FROM tmp_family_parent fp
JOIN tmp_family_child fc
  ON fp.family_id = fc.family_id;

-- Map parent-child relationships for sibling detection
CREATE TEMP TABLE tmp_parent_child_map AS
SELECT DISTINCT
  fp.parent_id,
  fc.child_id
FROM tmp_family_parent fp
JOIN tmp_family_child fc
  ON fp.family_id = fc.family_id;

-- Identify sibling pairs and count shared parents
CREATE TEMP TABLE tmp_sibling_pair AS
SELECT
  pcm1.child_id AS person_a,
  pcm2.child_id AS person_b,
  count(DISTINCT pcm1.parent_id) AS shared_parent_count
FROM tmp_parent_child_map pcm1
JOIN tmp_parent_child_map pcm2
  ON pcm1.parent_id = pcm2.parent_id
 AND pcm1.child_id < pcm2.child_id
GROUP BY pcm1.child_id, pcm2.child_id;

-- Build sibling relationship edges in both directions
CREATE TEMP TABLE tmp_sibling_edge AS
SELECT
  sp.person_a AS source_person_id,
  sp.person_b AS target_person_id,
  CASE
    WHEN sp.shared_parent_count >= 2 THEN 'SIBLING'
    ELSE 'HALF_SIBLING'
  END AS relationship_role_code,
  coalesce(greatest(pa.birth_date, pb.birth_date), pa.birth_date, pb.birth_date) AS start_date,
  CAST(NULL AS DATE) AS end_date,
  CAST(NULL AS VARCHAR) AS end_reason
FROM tmp_sibling_pair sp
LEFT JOIN tmp_person_latest pa
  ON pa.person_id = sp.person_a
LEFT JOIN tmp_person_latest pb
  ON pb.person_id = sp.person_b

UNION ALL

SELECT
  sp.person_b AS source_person_id,
  sp.person_a AS target_person_id,
  CASE
    WHEN sp.shared_parent_count >= 2 THEN 'SIBLING'
    ELSE 'HALF_SIBLING'
  END AS relationship_role_code,
  coalesce(greatest(pa.birth_date, pb.birth_date), pa.birth_date, pb.birth_date) AS start_date,
  CAST(NULL AS DATE) AS end_date,
  CAST(NULL AS VARCHAR) AS end_reason
FROM tmp_sibling_pair sp
LEFT JOIN tmp_person_latest pa
  ON pa.person_id = sp.person_a
LEFT JOIN tmp_person_latest pb
  ON pb.person_id = sp.person_b;

-- Combine all relationship types
CREATE TEMP TABLE tmp_combined_edge AS
SELECT * FROM tmp_spouse_edge
UNION ALL
SELECT * FROM tmp_parent_child_edge
UNION ALL
SELECT * FROM tmp_sibling_edge;

-- Deduplicate relationship edges
CREATE TEMP TABLE tmp_distinct_edge AS
SELECT DISTINCT
  source_person_id,
  target_person_id,
  relationship_role_code,
  start_date,
  end_date,
  end_reason
FROM tmp_combined_edge;

-- Enrich edges with dimension keys
CREATE TEMP TABLE tmp_enriched_edge AS
SELECT
  sp.person_sk AS source_person_sk,
  tp.person_sk AS target_person_sk,
  drr.relationship_role_sk,
  dd_start.date_sk AS start_date_sk,
  dd_end.date_sk AS end_date_sk,
  de.source_person_id,
  de.target_person_id,
  de.relationship_role_code,
  de.start_date,
  de.end_date,
  de.end_reason
FROM tmp_distinct_edge de
JOIN dim_person sp
  ON sp.person_id = de.source_person_id
 AND sp.is_current = TRUE
JOIN dim_person tp
  ON tp.person_id = de.target_person_id
 AND tp.is_current = TRUE
JOIN dim_relationship_role drr
  ON drr.relationship_role_code = de.relationship_role_code
LEFT JOIN dim_date dd_start
  ON dd_start.full_date = de.start_date
LEFT JOIN dim_date dd_end
  ON dd_end.full_date = de.end_date;

-- Insert new relationships not yet in fact table
INSERT INTO fact_person_relationship (
  source_person_sk,
  target_person_sk,
  relationship_role_sk,
  start_date_sk,
  end_date_sk,
  source_person_id,
  target_person_id,
  source_relationship_role_code,
  start_date,
  end_date,
  end_reason,
  first_seen_run_id,
  last_seen_run_id,
  is_deleted,
  relation_count,
  updated_at
)
SELECT
  ee.source_person_sk,
  ee.target_person_sk,
  ee.relationship_role_sk,
  ee.start_date_sk,
  ee.end_date_sk,
  ee.source_person_id,
  ee.target_person_id,
  ee.relationship_role_code,
  ee.start_date,
  ee.end_date,
  ee.end_reason,
  (SELECT run_id FROM tmp_current_run),
  (SELECT run_id FROM tmp_current_run),
  FALSE,
  1,
  NOW()
FROM tmp_enriched_edge ee
LEFT JOIN fact_person_relationship fct
  ON fct.source_person_id = ee.source_person_id
 AND fct.target_person_id = ee.target_person_id
 AND fct.source_relationship_role_code = ee.relationship_role_code
 AND fct.start_date IS NOT DISTINCT FROM ee.start_date
 AND fct.end_date IS NOT DISTINCT FROM ee.end_date
 AND fct.end_reason IS NOT DISTINCT FROM ee.end_reason
WHERE fct.relationship_sk IS NULL;

-- Update existing relationships with latest data
UPDATE fact_person_relationship fct
SET source_person_sk = ee.source_person_sk,
    target_person_sk = ee.target_person_sk,
    relationship_role_sk = ee.relationship_role_sk,
    start_date_sk = ee.start_date_sk,
    end_date_sk = ee.end_date_sk,
    last_seen_run_id = (SELECT run_id FROM tmp_current_run),
    is_deleted = FALSE,
    deleted_at_run_id = NULL,
    updated_at = NOW()
FROM tmp_enriched_edge ee
WHERE fct.source_person_id = ee.source_person_id
  AND fct.target_person_id = ee.target_person_id
  AND fct.source_relationship_role_code = ee.relationship_role_code
  AND fct.start_date IS NOT DISTINCT FROM ee.start_date
  AND fct.end_date IS NOT DISTINCT FROM ee.end_date
  AND fct.end_reason IS NOT DISTINCT FROM ee.end_reason;

-- Mark relationships as deleted if no longer in source
UPDATE fact_person_relationship fct
SET is_deleted = TRUE,
    deleted_at_run_id = (SELECT run_id FROM tmp_current_run),
    updated_at = NOW()
WHERE NOT EXISTS (
  SELECT 1
  FROM tmp_distinct_edge de
  WHERE de.source_person_id = fct.source_person_id
    AND de.target_person_id = fct.target_person_id
    AND de.relationship_role_code = fct.source_relationship_role_code
    AND de.start_date IS NOT DISTINCT FROM fct.start_date
    AND de.end_date IS NOT DISTINCT FROM fct.end_date
    AND de.end_reason IS NOT DISTINCT FROM fct.end_reason
)
  AND fct.is_deleted = FALSE;

-- Clean up temporary tables
DROP TABLE tmp_enriched_edge;
DROP TABLE tmp_distinct_edge;
DROP TABLE tmp_combined_edge;
DROP TABLE tmp_sibling_edge;
DROP TABLE tmp_sibling_pair;
DROP TABLE tmp_parent_child_map;
DROP TABLE tmp_parent_child_edge;
DROP TABLE tmp_family_child;
DROP TABLE tmp_family_parent;
DROP TABLE tmp_spouse_edge;
DROP TABLE tmp_family_spouse;
DROP TABLE tmp_relation_latest;
DROP TABLE tmp_family_latest;
DROP TABLE tmp_person_latest;
DROP TABLE tmp_current_run;
