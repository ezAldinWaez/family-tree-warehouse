// =============================================================================
// SCENARIO 02 — Life-Event Updates: Deaths, Divorce, Career & Education Changes
// =============================================================================
//
// Summary of changes:
//   (A) Emma Young   (p9)  passes away on 2026-02-10         -> death_date set
//   (B) Timothy Young (p16) & Christina Cruz (p15) divorce   -> f8 status -> DIVORCED
//   (C) Carrie Young  (p27) finishes university, starts job  -> education & works updated
//   (D) Maria Young   (p22) gets promoted                    -> works updated
//
// Expected DW effect after running the Airflow DAG:
//   dim_person (SCD Type 2 — attr_hash changes trigger old-row expire + new-row insert):
//     p9  — death_date added       -> old row is_current=FALSE, new row is_current=TRUE
//     p27 — education+works change -> old row is_current=FALSE, new row is_current=TRUE
//     p22 — works change           -> old row is_current=FALSE, new row is_current=TRUE
//     NOTE: p16 & p15 are NOT flipped — the divorce only touches the Family node,
//           not Person attributes, so their attr_hash is unchanged.
//
//   fact_person_relationship (divorce of f8):
//     The row dedup key includes (start_date, end_date, end_reason).
//     Before: end_date=NULL,       end_reason=NULL
//     After : end_date=2026-03-01, end_reason='DIVORCE'
//     -> The old spouse rows (p16<->p15, both directions) DO NOT match -> is_deleted=TRUE
//     -> Two new rows are inserted:  SPOUSE_HUSBAND p16->p15  (end_date=2026-03-01, end_reason='DIVORCE')
//                                    SPOUSE_WIFE    p15->p16  (end_date=2026-03-01, end_reason='DIVORCE')
//     Sibling edges (p27<->p28, p28<->p29, p27<->p29) are unaffected — they are derived
//     from birth dates, which did not change.
// =============================================================================


// ---------------------------------------------------------------------------
// (A) Death: Emma Young (p9, born 1950) passes away
// ---------------------------------------------------------------------------

MATCH (p9:Person { id: 'p9' })
SET p9.death_date = date('2026-02-10');


// ---------------------------------------------------------------------------
// (B) Divorce: Timothy Young (p16) & Christina Cruz (p15) — Family f8
// ---------------------------------------------------------------------------

MATCH (f8:Family { id: 'f8' })
SET f8.status       = 'DIVORCED',
    f8.divorce_date = date('2026-03-01');


// ---------------------------------------------------------------------------
// (C) Career & Education Update: Carrie Young (p27) graduates and starts working
// ---------------------------------------------------------------------------

MATCH (p27:Person { id: 'p27' })
SET p27.education = 'BSc Psychology',
    p27.works     = 'Social Worker';


// ---------------------------------------------------------------------------
// (D) Promotion: Maria Young (p22) advances in her research career
// ---------------------------------------------------------------------------

MATCH (p22:Person { id: 'p22' })
SET p22.works = 'Senior Researcher';
