-- Create sequence for relationship role surrogate key if not exists
CREATE SEQUENCE IF NOT EXISTS seq_relationship_role_sk START 1;

-- Create relationship role dimension table if not exists
CREATE TABLE IF NOT EXISTS dim_relationship_role (
  relationship_role_sk BIGINT PRIMARY KEY DEFAULT nextval('seq_relationship_role_sk'),
  relationship_role_code VARCHAR UNIQUE NOT NULL,
  relationship_type VARCHAR NOT NULL,
  person_role VARCHAR NOT NULL,
  is_symmetric BOOLEAN NOT NULL DEFAULT FALSE,
  inverse_role_code VARCHAR
);

-- Populate relationship role dimension with all relationship types
INSERT INTO dim_relationship_role (
	relationship_role_code,
	relationship_type,
	person_role,
	is_symmetric,
	inverse_role_code
)
VALUES
	('SPOUSE_HUSBAND', 'SPOUSE', 'HUSBAND', FALSE, 'SPOUSE_WIFE'),
	('SPOUSE_WIFE', 'SPOUSE', 'WIFE', FALSE, 'SPOUSE_HUSBAND'),
	('SPOUSE_PARTNER', 'SPOUSE', 'PARTNER', TRUE, 'SPOUSE_PARTNER'),
	('PARENT', 'PARENT_CHILD', 'PARENT', FALSE, 'CHILD'),
	('CHILD', 'PARENT_CHILD', 'CHILD', FALSE, 'PARENT'),
	('SIBLING', 'SIBLING', 'SIBLING', TRUE, 'SIBLING'),
	('HALF_SIBLING', 'SIBLING', 'HALF_SIBLING', TRUE, 'HALF_SIBLING')
ON CONFLICT DO NOTHING;