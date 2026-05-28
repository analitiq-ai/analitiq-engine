-- Canonical seed table used by every database in the E2E matrix.
-- Per-database specs translate the DDL into their native types where it
-- doesn't match exactly (e.g. SQLite has no native TIMESTAMP, Snowflake
-- has its own type vocabulary). This file is the spec, not a runnable
-- script in itself.

CREATE TABLE e2e_seed_data (
    id          INTEGER     PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    score       INTEGER,
    created_at  TIMESTAMP   NOT NULL,
    updated_at  TIMESTAMP   NOT NULL
);

INSERT INTO e2e_seed_data (id, name, email, score, created_at, updated_at) VALUES
    (1, 'Alice',    'alice@example.com',    95, '2026-01-01 00:00:00', '2026-01-01 00:00:00'),
    (2, 'Bob',      'bob@example.com',      80, '2026-01-02 00:00:00', '2026-01-02 00:00:00'),
    (3, 'Charlie',  'charlie@example.com',  88, '2026-01-03 00:00:00', '2026-01-03 00:00:00'),
    (4, 'Dave',     'dave@example.com',     72, '2026-01-04 00:00:00', '2026-01-04 00:00:00'),
    (5, 'Eve',      'eve@example.com',      91, '2026-01-05 00:00:00', '2026-01-05 00:00:00');
