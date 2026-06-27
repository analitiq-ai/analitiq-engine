-- Resume-accuracy delta for the incremental e2e (Postgres).
-- NOT auto-run on container start. Apply it by hand BETWEEN the two runs of an
-- incremental pipeline to add rows past the cursor the first run saved, then
-- run the pipeline again and confirm only these rows move (see README,
-- "Resume accuracy (incremental)"). ids 6,7 sit above the id=5 / 2026-02-05
-- high-water mark, so an incremental stream cursored on id or updated_at picks
-- up exactly these on the second run.

INSERT INTO e2e_seed (id, name, email, score, created_at, updated_at) VALUES
    (6, 'Frank', 'frank@example.com', 88, '2026-01-06 12:00:00.110011', '2026-02-06 12:00:00.110011'),
    (7, 'Grace', 'grace@example.com', 64, '2026-01-07 12:00:00.120012', '2026-02-07 12:00:00.120012');
