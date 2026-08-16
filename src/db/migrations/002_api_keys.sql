-- Storage for the optional authentication feature.
--
-- The table exists unconditionally so the schema is identical in both
-- configurations; it simply stays empty and unread while AUTH_ENABLED=false.
--
-- Keys are stored as SHA-256 hashes, never in plaintext: a database backup or
-- an accidental SELECT should not hand over working credentials. Seeding is an
-- upsert on the hash, which is what makes restarting the service safe -- the
-- seeded load-generator key survives untouched.

CREATE TABLE IF NOT EXISTS api_keys (
    key_hash   text        PRIMARY KEY,
    name       text        NOT NULL,
    scopes     text[]      NOT NULL DEFAULT ARRAY['ingest', 'query'],
    created_at timestamptz NOT NULL DEFAULT now()
);
