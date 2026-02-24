// =============================================================================
// SCENARIO 01 — Incremental Inserts: New Births & A New Marriage
// =============================================================================
//
// Summary of changes:
//   (A) New baby 'Leo Cruz'  (p37) born 2026-01-15  ->  CHILD_OF f10 (Steve Cruz & Arham Chopra)
//   (B) New baby 'Lily Young' (p38) born 2025-06-20 ->  CHILD_OF f8  (Timothy Young & Christina Cruz)
//   (C) Cullen Cruz (p17) marries new person 'Grace Lin' (p39) -> new Family f13
//
// Expected DW effect after running the Airflow DAG:
//   dim_person (3 new rows): p37, p38, p39
//     — p17 (Cullen) gets NO new dim_person row; attributes unchanged, relationships
//       do not affect the SCD2 hash.
//
//   fact_person_relationship (22 new rows):
//     Via f10 — p37 (Leo):
//       PARENT : p19->p37, p21->p37
//       CHILD  : p37->p19, p37->p21
//       SIBLING: p37<->p30, p37<->p31  (2 shared parents -> SIBLING, not HALF_SIBLING)
//       HALF_SIBLING: p37<->p26 (1 shared parent -> HALF_SIBLING)
//     Via f8  — p38 (Lily):
//       PARENT : p16->p38, p15->p38
//       CHILD  : p38->p16, p38->p15
//       SIBLING: p38<->p27, p38<->p28, p38<->p29  (2 shared parents each)
//     Via f13 — (Cullen & Grace):
//       SPOUSE_HUSBAND: p17->p39
//       SPOUSE_WIFE   : p39->p17
// =============================================================================


// ---------------------------------------------------------------------------
// (A) New Baby: Leo Cruz — born to Steve Cruz (p19) & Arham Chopra (p21) / f10
// ---------------------------------------------------------------------------

MERGE (p37:Person { id: 'p37' })
ON CREATE SET
  p37.first_name  = 'Leo',
  p37.last_name   = 'Cruz',
  p37.sex         = 'Male',
  p37.birth_date  = date('2026-01-15'),
  p37.education   = 'None',
  p37.works       = 'Infant';

MATCH (p37:Person { id: 'p37' }), (f10:Family { id: 'f10' })
MERGE (p37)-[:CHILD_OF]->(f10);


// ---------------------------------------------------------------------------
// (B) New Baby: Lily Young — born to Timothy Young (p16) & Christina Cruz (p15) / f8
// ---------------------------------------------------------------------------

MERGE (p38:Person { id: 'p38' })
ON CREATE SET
  p38.first_name  = 'Lily',
  p38.last_name   = 'Young',
  p38.sex         = 'Female',
  p38.birth_date  = date('2025-06-20'),
  p38.education   = 'None',
  p38.works       = 'Infant';

MATCH (p38:Person { id: 'p38' }), (f8:Family { id: 'f8' })
MERGE (p38)-[:CHILD_OF]->(f8);


// ---------------------------------------------------------------------------
// (C) New Marriage: Cullen Cruz (p17) weds Grace Lin (p39) -> Family f13
// ---------------------------------------------------------------------------

MERGE (p39:Person { id: 'p39' })
ON CREATE SET
  p39.first_name  = 'Grace',
  p39.last_name   = 'Lin',
  p39.sex         = 'Female',
  p39.birth_date  = date('1978-03-10'),
  p39.education   = 'Architecture Degree',
  p39.works       = 'Urban Planner';

MERGE (f13:Family { id: 'f13' })
ON CREATE SET
  f13.marriage_date = date('2006-08-18'),
  f13.status        = 'ACTIVE';

MATCH (p17:Person { id: 'p17' }), (f13:Family { id: 'f13' })
MERGE (p17)-[:SPOUSE_OF { role: 'HUSBAND' }]->(f13);

MATCH (p39:Person { id: 'p39' }), (f13:Family { id: 'f13' })
MERGE (p39)-[:SPOUSE_OF { role: 'WIFE' }]->(f13);
