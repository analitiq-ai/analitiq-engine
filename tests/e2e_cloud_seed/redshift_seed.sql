-- Seed the canonical 5-row e2e table in Amazon Redshift.
-- Run against the Redshift connection 3333...-3333 before running the
-- e2e-redshift-to-postgres pipeline.
--
--   psql "$REDSHIFT_URL" -f tests/e2e_cloud_seed/redshift_seed.sql

DROP TABLE IF EXISTS public.e2e_seed;
CREATE TABLE public.e2e_seed (
    id          integer      NOT NULL,
    name        varchar(100) NOT NULL,
    email       varchar(255) NOT NULL,
    score       integer,
    created_at  timestamp    NOT NULL,
    updated_at  timestamp    NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO public.e2e_seed (id, name, email, score, created_at, updated_at) VALUES
    (1, 'Alice',   'alice@example.com',   95,   '2026-01-01 08:30:00.100001', '2026-02-01 09:15:00.200002'),
    (2, 'Bob',     'bob@example.com',     80,   '2026-01-02 10:00:00.300003', '2026-02-02 11:45:00.400004'),
    (3, 'Charlie', 'charlie@example.com', NULL, '2026-01-03 14:20:00.500005', '2026-02-03 07:05:00.600006'),
    (4, 'Dave',    'dave@example.com',    72,   '2026-01-04 06:00:00.700007', '2026-02-04 18:30:00.800008'),
    (5, 'Eve',     'eve@example.com',     91,   '2026-01-05 23:59:00.900009', '2026-02-05 00:01:00.010010');
