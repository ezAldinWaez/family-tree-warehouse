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