INSERT INTO dim_relationship_type(relationship_code)
VALUES ('SPOUSE_OF'), ('CHILD_OF')
ON CONFLICT DO NOTHING;

INSERT INTO dim_role(role_code)
VALUES ('HUSBAND'), ('WIFE'), ('NONE')
ON CONFLICT DO NOTHING;