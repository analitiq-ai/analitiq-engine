-- Seed the canonical 5-row e2e table in BigQuery.
-- Run against the BigQuery connection 2222...-2222 before running the
-- e2e-bigquery-to-postgres pipeline. The dataset matches that connection's
-- dataset_id.
--
--   bq query --use_legacy_sql=false < tests/e2e_cloud_seed/bigquery_seed.sql

CREATE OR REPLACE TABLE `analitiq_e2e_testing.e2e_seed` (
    id         INT64    NOT NULL,
    name       STRING   NOT NULL,
    email      STRING   NOT NULL,
    score      INT64,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);

INSERT INTO `analitiq_e2e_testing.e2e_seed` (id, name, email, score, created_at, updated_at) VALUES
    (1, 'Alice',   'alice@example.com',   95,   DATETIME '2026-01-01 08:30:00.100001', DATETIME '2026-02-01 09:15:00.200002'),
    (2, 'Bob',     'bob@example.com',     80,   DATETIME '2026-01-02 10:00:00.300003', DATETIME '2026-02-02 11:45:00.400004'),
    (3, 'Charlie', 'charlie@example.com', NULL, DATETIME '2026-01-03 14:20:00.500005', DATETIME '2026-02-03 07:05:00.600006'),
    (4, 'Dave',    'dave@example.com',    72,   DATETIME '2026-01-04 06:00:00.700007', DATETIME '2026-02-04 18:30:00.800008'),
    (5, 'Eve',     'eve@example.com',     91,   DATETIME '2026-01-05 23:59:00.900009', DATETIME '2026-02-05 00:01:00.010010');
