-- =============================================================================
-- Report 03: Ancestor Lineage Trace
-- =============================================================================
-- Recursively walks CHILD edges upward from every current person to find all
-- their ancestors, annotating each hop with the generation depth and a
-- readable lineage path string.
--
-- Demonstrates: recursive CTE over the factless fact table. Because the fact
-- table stores the CHILD role as (source=child -> target=parent), each
-- recursive step simply follows that edge type upward.
--
-- generation_depth = 1 -> parent
-- generation_depth = 2 -> grandparent
-- generation_depth = 3 -> great-grandparent  ... etc.
-- =============================================================================

WITH RECURSIVE ancestor_chain AS (

    -- -------------------------------------------------------------------------
    -- Base case: every current, non-deleted person is their own generation-0
    -- -------------------------------------------------------------------------
    SELECT
        p.person_id                                          AS root_person_id,
        p.first_name || ' ' || p.last_name                   AS root_person_name,
        p.birth_date                                         AS root_birth_date,
        p.person_id                                          AS ancestor_person_id,
        p.first_name || ' ' || p.last_name                   AS ancestor_name,
        p.birth_date                                         AS ancestor_birth_date,
        p.death_date                                         AS ancestor_death_date,
        0                                                    AS generation_depth,
        CAST(p.first_name || ' ' || p.last_name AS VARCHAR)  AS lineage_path
    FROM dim_person p
    WHERE p.is_current = TRUE AND p.is_deleted = FALSE

    UNION ALL

    -- -------------------------------------------------------------------------
    -- Recursive step: follow CHILD edge (source=child, target=parent) upward
    -- -------------------------------------------------------------------------
    SELECT
        ac.root_person_id,
        ac.root_person_name,
        ac.root_birth_date,
        parent.person_id                                                          AS ancestor_person_id,
        parent.first_name || ' ' || parent.last_name                              AS ancestor_name,
        parent.birth_date                                                         AS ancestor_birth_date,
        parent.death_date                                                         AS ancestor_death_date,
        ac.generation_depth + 1                                                   AS generation_depth,
        ac.lineage_path || ' -> ' || parent.first_name || ' ' || parent.last_name AS lineage_path
    FROM ancestor_chain ac
    JOIN fact_person_relationship fpr ON fpr.source_person_id = ac.ancestor_person_id AND fpr.source_relationship_role_code = 'CHILD' AND fpr.is_deleted = FALSE
    JOIN dim_person parent ON parent.person_id = fpr.target_person_id AND parent.is_current = TRUE
)

SELECT
    root_person_name                              AS person,
    root_birth_date                               AS person_birth_date,
    generation_depth                              AS generations_back,
    CASE generation_depth
        WHEN 1 THEN 'Parent'
        WHEN 2 THEN 'Grandparent'
        WHEN 3 THEN 'Great-grandparent'
        ELSE        'Great-' || REPEAT('great-', generation_depth - 3) || 'grandparent'
    END                                           AS relation_label,
    ancestor_name                                 AS ancestor,
    ancestor_birth_date                           AS ancestor_birth_date,
    ancestor_death_date                           AS ancestor_death_date,
    lineage_path                                  AS lineage_path

FROM ancestor_chain
WHERE generation_depth > 0

ORDER BY
    root_birth_date     DESC,   -- youngest people first
    root_person_name,
    generation_depth    ASC;
