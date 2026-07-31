-- ClickHouse integration fixture for the SQL API (specs/sql_api_clickhouse_spec.md §2).
--
-- This file IS the canonical grants matrix: every CREATE/GRANT below matches the
-- deployment documentation verbatim, so the exact permissions a data role and a
-- connection user need are executable and versioned. Executed automatically by the
-- `clickhouse` service in docker-compose.local.yaml on first start
-- (mounted at /docker-entrypoint-initdb.d/).
--
-- Matrix (spec §2):
--   data role  -> GRANT SELECT ON <db>.* TO <rol>;
--   conn user  -> GRANT <rol> TO <usuario>;  (one per allowlisted role)
--   conn user  -> ALTER USER <usuario> DEFAULT ROLE NONE;  (no privileges without a role)
--   startup probe (nonexistent sentinel role) needs NO grant.

-- ── Test database and table (heterogeneous types: serialization ground truth) ──

CREATE DATABASE IF NOT EXISTS loom_it;

CREATE TABLE IF NOT EXISTS loom_it.events
(
    id         UInt32,
    created_at DateTime64(3, 'UTC'),
    amount     Decimal(18, 4),
    note       Nullable(String),
    flags      Array(UInt8)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO loom_it.events VALUES
    (1, '2026-01-01 00:00:00.123', 10.5000, 'first', [1, 2, 3]),
    (2, '2026-01-02 12:30:45.678', -2.2500, NULL, []),
    (3, '2026-02-15 08:00:00.001', 9999999.9999, 'third', [255]),
    (4, '2026-03-01 23:59:59.999', 0.0001, 'fourth', [0, 128]),
    (5, '2026-07-04 16:20:00.500', 123.4567, NULL, [42]);

-- Second database readable ONLY by loom_reader_b, so a query carrying both roles
-- can be proven to get the UNION of their privileges (neither role alone reads both).

CREATE DATABASE IF NOT EXISTS loom_it_b;

CREATE TABLE IF NOT EXISTS loom_it_b.events_b (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO loom_it_b.events_b VALUES (10), (20);

-- ── Data roles (each gets EXACTLY the grant from the matrix) ──────────────────

-- In the connection allowlist AND granted to the connection user.
CREATE ROLE IF NOT EXISTS loom_reader_a;
GRANT SELECT ON loom_it.* TO loom_reader_a;

-- Also granted to the connection user; the only role that reads loom_it_b.
CREATE ROLE IF NOT EXISTS loom_reader_b;
GRANT SELECT ON loom_it.* TO loom_reader_b;
GRANT SELECT ON loom_it_b.* TO loom_reader_b;

-- Exists (with data access) but NOT granted to the connection user:
-- the server must reject it per query (SET_NON_GRANTED_ROLE, code 512).
CREATE ROLE IF NOT EXISTS loom_ungranted;
GRANT SELECT ON loom_it.* TO loom_ungranted;

-- ── Connection user (minimal: no default privileges, only allowlisted roles) ──

CREATE USER IF NOT EXISTS loom_api IDENTIFIED WITH plaintext_password BY 'loom_api_pw'
    DEFAULT ROLE NONE;
GRANT loom_reader_a, loom_reader_b TO loom_api;
ALTER USER loom_api DEFAULT ROLE NONE;
