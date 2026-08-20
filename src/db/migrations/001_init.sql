-- Core schema for the log ingestion and query service.

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ---------------------------------------------------------------------------
-- Service dictionary
-- ---------------------------------------------------------------------------
-- Service names are inherently low cardinality (tens, occasionally thousands)
-- but would otherwise be repeated verbatim on every one of millions of rows.
-- Storing a 4-byte id instead of a ~12-byte string keeps more of the table
-- resident in a 256 MB shared_buffers, and turns both `service=` filters and
-- `group_by=service` into integer comparisons.
--
-- The application caches this table in memory, so `group_by=service` resolves
-- names locally and the query needs no join at all.
CREATE TABLE IF NOT EXISTS services (
    service_id serial PRIMARY KEY,
    name       text NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------------
-- Row id allocation
-- ---------------------------------------------------------------------------
-- INCREMENT BY 10000 makes this a hi/lo block allocator: one nextval() call
-- reserves a 10,000-id block for the caller, so the application allocates ids
-- locally and touches the sequence roughly once per second at target load
-- instead of once per row. Correct across processes and restarts by
-- construction, since no two callers can receive the same block.
CREATE SEQUENCE IF NOT EXISTS logs_id_seq AS bigint INCREMENT BY 10000 START WITH 1 CACHE 1;

-- ---------------------------------------------------------------------------
-- Logs
-- ---------------------------------------------------------------------------
-- Partitioned by day on ts. Three reasons, in order of importance:
--
--   1. Retention becomes DETACH + DROP TABLE: an O(1) catalog operation that
--      reclaims space instantly. A bulk DELETE would instead leave millions of
--      dead tuples for autovacuum to grind through, bloating both heap and
--      index while competing with ingestion for the single available CPU.
--   2. Time-bounded queries prune to the partitions actually in range.
--   3. Index maintenance stays cheap: inserts land in the small, hot,
--      fully-cached btree of the current day rather than a month-wide one.
--
-- Column order places the fixed-width columns first, packing them into 24 bytes
-- (8 + 8 + 4 + 2 + 2 padding) with no interior alignment holes.
CREATE TABLE IF NOT EXISTS logs (
    id         bigint      NOT NULL,
    ts         timestamptz NOT NULL,
    service_id integer     NOT NULL,
    level      smallint    NOT NULL,
    message    text        NOT NULL,
    attributes jsonb       NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT logs_level_valid CHECK (level BETWEEN 0 AND 3)
) PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS logs_ts_id_desc_idx ON logs (ts DESC, id DESC) INCLUDE (service_id, level);

CREATE TABLE IF NOT EXISTS logs_default PARTITION OF logs DEFAULT;
