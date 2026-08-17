-- Per-database planner and executor settings.
--
-- These are already in db/postgresql.conf, but that file only applies when the
-- service runs against the PostgreSQL container defined in docker-compose.yml.
-- Point the app at a stock server -- a managed instance, a CI service container,
-- someone else's compose file -- and every one of those decisions is silently
-- lost, turning a tuned service into an untuned one with no error to explain the
-- drop in throughput.
--
-- ALTER DATABASE settings are stored in the catalog and applied to every new
-- connection, so they travel with the database rather than the deployment.
-- Repeating them here is deliberate redundancy, not duplication.
--
-- Only settings valid at database scope belong here. shared_buffers, max_wal_size,
-- checkpoint and autovacuum tuning are postmaster- or table-scoped and stay in
-- postgresql.conf. synchronous_commit is set per session by the ingest writers,
-- so it is already independent of the server config.

DO $$
DECLARE
    target text := current_database();
BEGIN
    -- JIT compilation adds fixed latency to every aggregation and buys nothing
    -- at this data size; it is a measurable p95 regression on a 1 CPU budget.
    EXECUTE format('ALTER DATABASE %I SET jit = off', target);

    -- With a 1 CPU quota, parallel workers split the same core and add
    -- coordination overhead. Serial plans are faster and far more predictable.
    EXECUTE format('ALTER DATABASE %I SET max_parallel_workers_per_gather = 0', target);

    -- Partitionwise aggregation builds a separate Sort plus Partial
    -- GroupAggregate per partition. Measured here it produced a 252-node plan
    -- and lost badly to a single HashAggregate over an Append.
    EXECUTE format('ALTER DATABASE %I SET enable_partitionwise_aggregate = off', target);
    EXECUTE format('ALTER DATABASE %I SET enable_partitionwise_join = off', target);

    -- Execution-time pruning is what keeps a time-bounded query touching only
    -- the days in range.
    EXECUTE format('ALTER DATABASE %I SET enable_partition_pruning = on', target);

    -- Aggregation hash and sort space. Result sets here are small; this is sized
    -- to keep them in memory without risking the 1 GB budget across backends.
    EXECUTE format('ALTER DATABASE %I SET work_mem = ''8MB''', target);

    -- SSD-backed storage: random access is nearly as cheap as sequential, and
    -- the default of 4.0 pushes the planner towards sequential scans it should
    -- not choose.
    EXECUTE format('ALTER DATABASE %I SET random_page_cost = 1.1', target);
END
$$;
