-- Auto-run by the mysql image on first container start
-- (mounted into /docker-entrypoint-initdb.d). Creates and seeds the canonical
-- e2e source table in the e2e_db database. DATETIME(6) keeps microseconds.
-- The destination table (e2e_landing) is created by the engine.

USE e2e_db;

CREATE TABLE e2e_seed (
    id          INT          NOT NULL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    score       INT,
    created_at  DATETIME(6)  NOT NULL,
    updated_at  DATETIME(6)  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO e2e_seed (id, name, email, score, created_at, updated_at) VALUES
    (1, 'Alice',   'alice@example.com',   95,   '2026-01-01 08:30:00.100001', '2026-02-01 09:15:00.200002'),
    (2, 'Bob',     'bob@example.com',     80,   '2026-01-02 10:00:00.300003', '2026-02-02 11:45:00.400004'),
    (3, 'Charlie', 'charlie@example.com', NULL, '2026-01-03 14:20:00.500005', '2026-02-03 07:05:00.600006'),
    (4, 'Dave',    'dave@example.com',    72,   '2026-01-04 06:00:00.700007', '2026-02-04 18:30:00.800008'),
    (5, 'Eve',     'eve@example.com',     91,   '2026-01-05 23:59:00.900009', '2026-02-05 00:01:00.010010');
