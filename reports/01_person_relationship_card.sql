-- =============================================================================
-- Report 01: Person Relationship Card
-- =============================================================================
-- For every current, non-deleted person, lists all their active relationships
-- (PARENT, CHILD, SIBLING, HALF_SIBLING, SPOUSE_HUSBAND, SPOUSE_WIFE).
-- One row per (person, role, related person).
--
-- Demonstrates: the factless fact table as a relationship roster — the core
-- value of the DW design. Filtering is_deleted=FALSE gives the live state;
-- removing that filter shows the full history including corrections.
-- =============================================================================

SELECT
    -- Source person
    src.person_id                                          AS person_id,
    src.first_name || ' ' || src.last_name                 AS person,
    src.birth_date                                         AS person_birth_date,
    src.death_date                                         AS person_death_date,

    -- Relationship
    drr.relationship_type                                  AS relationship_type,
    fpr.source_relationship_role_code                      AS role,
    fpr.start_date                                         AS relationship_start,
    fpr.end_date                                           AS relationship_end,
    fpr.end_reason                                         AS end_reason,

    -- Related person
    tgt.person_id                                          AS related_person_id,
    tgt.first_name || ' ' || tgt.last_name                 AS related_person,
    tgt.birth_date                                         AS related_birth_date,
    tgt.death_date                                         AS related_death_date

FROM fact_person_relationship  fpr
JOIN dim_person src ON src.person_sk = fpr.source_person_sk AND src.is_current = TRUE
JOIN dim_person tgt ON tgt.person_sk = fpr.target_person_sk AND tgt.is_current = TRUE
JOIN dim_relationship_role drr ON drr.relationship_role_sk = fpr.relationship_role_sk

WHERE fpr.is_deleted = FALSE

ORDER BY
    src.last_name,
    src.first_name,
    drr.relationship_type,
    fpr.source_relationship_role_code,
    tgt.birth_date;
