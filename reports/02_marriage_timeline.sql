-- =============================================================================
-- Report 02: Marriage Timeline
-- =============================================================================
-- One row per married couple (deduped to SPOUSE_HUSBAND direction to avoid
-- double-counting). Shows marriage year, years together, and outcome derived
-- from the fact table end_date / end_reason and the dim_person death_dates.
--
-- Demonstrates: time-intelligence using dim_date + SCD2 dim_person.
-- The "years together" column changes automatically on re-run as time passes
-- because it uses CURRENT_DATE for still-active marriages.
-- =============================================================================

SELECT
    -- Couple
    husband.person_id                                         AS husband_id,
    husband.first_name || ' ' || husband.last_name            AS husband,
    wife.person_id                                            AS wife_id,
    wife.first_name    || ' ' || wife.last_name               AS wife,

    -- Time axis (via dim_date for the marriage year)
    dd_marriage.year_num                                      AS marriage_year,
    fpr.start_date                                            AS marriage_date,

    -- Outcome
    fpr.end_date                                              AS divorce_date,
    fpr.end_reason                                            AS end_reason,

    CASE
        WHEN fpr.end_reason = 'DIVORCE' THEN 'Divorced'
        WHEN husband.death_date IS NOT NULL AND wife.death_date IS NOT NULL THEN 'Both deceased'
        WHEN husband.death_date IS NOT NULL THEN 'Husband deceased'
        WHEN wife.death_date IS NOT NULL THEN 'Wife deceased'
        ELSE 'Active'
    END                                                       AS outcome,

    -- Duration: until divorce, until first death, or until today
    date_diff(
        'year',
        fpr.start_date,
        COALESCE(
            fpr.end_date,                                      -- divorced
            NULLIF(LEAST(
                COALESCE(husband.death_date, DATE '9999-12-31'),
                COALESCE(wife.death_date,    DATE '9999-12-31')
            ), DATE '9999-12-31'),                             -- first to die
            CURRENT_DATE                                       -- still active
        )
    )                                                         AS years_together

FROM fact_person_relationship  fpr
JOIN dim_relationship_role drr ON drr.relationship_role_sk = fpr.relationship_role_sk AND drr.relationship_role_code = 'SPOUSE_HUSBAND'
JOIN dim_person husband ON husband.person_sk = fpr.source_person_sk AND husband.is_current = TRUE
JOIN dim_person    wife ON    wife.person_sk = fpr.target_person_sk AND    wife.is_current = TRUE
LEFT JOIN dim_date dd_marriage ON dd_marriage.full_date = fpr.start_date

WHERE fpr.is_deleted = FALSE

ORDER BY fpr.start_date;
