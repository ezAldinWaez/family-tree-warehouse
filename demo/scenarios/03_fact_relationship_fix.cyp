// =============================================================================
// SCENARIO 03 — Fact / Relationship Fix: Child Moved to Correct Family
// =============================================================================
//
// Setup:
//   A new child 'Sam Cruz' (p40, born 2015) is added to the tree.
//   Due to a data-entry mistake the child is initially linked to the wrong family:
//     WRONG   -> p40 CHILD_OF f7  (Arjun Patel & Olivia Cruz)
//     CORRECT -> p40 CHILD_OF f8  (Timothy Young & Christina Cruz)
//
// How to observe the full DW effect:
//   1. Run STEP 1 + STEP 2 in Neo4j, then trigger the Airflow DAG.
//      -> dim_person : 1 new current row for p40
//      -> fact_person_relationship (wrong state via f7 — parents: p13 Arjun, p12 Olivia):
//          PARENT : p13->p40, p12->p40
//          CHILD  : p40->p13, p40->p12
//          SIBLING: p40<->p19  (p19 is the only other child of f7; 2 shared parents -> SIBLING)
//
//   2. Run STEP 3 + STEP 4 in Neo4j, then trigger the Airflow DAG again.
//      -> The wrong rows above are no longer in the snapshot -> is_deleted=TRUE
//      -> New correct rows for f8 (parents: p16 Timothy, p15 Christina) are inserted:
//          PARENT : p16->p40, p15->p40
//          CHILD  : p40->p16, p40->p15
//          SIBLING: p40<->p27, p40<->p28, p40<->p29  (all 3 existing children of f8)
// =============================================================================


// ---------------------------------------------------------------------------
// STEP 1: Add the new person — Sam Cruz
// ---------------------------------------------------------------------------

MERGE (p40:Person { id: 'p40' })
ON CREATE SET
  p40.first_name = 'Sam',
  p40.last_name  = 'Cruz',
  p40.sex        = 'Male',
  p40.birth_date = date('2015-07-04'),
  p40.education  = 'Primary School',
  p40.works      = 'Student';


// ---------------------------------------------------------------------------
// STEP 2: Wrong initial entry — Sam accidentally linked to f7
//         (Arjun Patel & Olivia Cruz — the grandparent generation)
// ---------------------------------------------------------------------------

MATCH (p40:Person { id: 'p40' }), (f7:Family { id: 'f7' })
MERGE (p40)-[:CHILD_OF]->(f7);

// >>> TRIGGER AIRFLOW DAG HERE — captures the wrong state in the warehouse <<<


// ---------------------------------------------------------------------------
// STEP 3: Correction — remove the wrong CHILD_OF relationship
// ---------------------------------------------------------------------------

MATCH (p40:Person { id: 'p40' })-[r:CHILD_OF]->(f7:Family { id: 'f7' })
DELETE r;


// ---------------------------------------------------------------------------
// STEP 4: Correction — link Sam to the correct family f8
//         (Timothy Young & Christina Cruz — his actual parents)
// ---------------------------------------------------------------------------

MATCH (p40:Person { id: 'p40' }), (f8:Family { id: 'f8' })
MERGE (p40)-[:CHILD_OF]->(f8);

// >>> TRIGGER AIRFLOW DAG HERE — warehouse corrects: old fact rows is_deleted=TRUE,
//     new correct fact rows inserted                                         <<<
