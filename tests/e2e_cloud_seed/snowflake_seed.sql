-- Seed the canonical 5-row e2e table in Snowflake.
-- Run against the Snowflake connection 1111...-1111 before running the
-- e2e-snowflake-to-postgres pipeline. Uses the database + PUBLIC schema
-- configured on that connection.
--
--   snowsql -c <conn> -f tests/e2e_cloud_seed/snowflake_seed.sql

CREATE OR REPLACE TABLE E2E_SEED (
    ID         NUMBER(38,0)     NOT NULL PRIMARY KEY,
    NAME       VARCHAR(100)     NOT NULL,
    EMAIL      VARCHAR(255)     NOT NULL,
    SCORE      NUMBER(38,0),
    CREATED_AT TIMESTAMP_NTZ(6) NOT NULL,
    UPDATED_AT TIMESTAMP_NTZ(6) NOT NULL
);

INSERT INTO E2E_SEED (ID, NAME, EMAIL, SCORE, CREATED_AT, UPDATED_AT) VALUES
    (1, 'Alice',   'alice@example.com',   95,   '2026-01-01 08:30:00.100001', '2026-02-01 09:15:00.200002'),
    (2, 'Bob',     'bob@example.com',     80,   '2026-01-02 10:00:00.300003', '2026-02-02 11:45:00.400004'),
    (3, 'Charlie', 'charlie@example.com', NULL, '2026-01-03 14:20:00.500005', '2026-02-03 07:05:00.600006'),
    (4, 'Dave',    'dave@example.com',    72,   '2026-01-04 06:00:00.700007', '2026-02-04 18:30:00.800008'),
    (5, 'Eve',     'eve@example.com',     91,   '2026-01-05 23:59:00.900009', '2026-02-05 00:01:00.010010');
