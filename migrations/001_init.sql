-- Sharyt — initial schema (Postgres).
--
-- Conventions:
--   * Money is BIGINT cents. Not INTEGER: that caps at 2,147,483,647, which is
--     fine for one line and overflows a settlement SUM(). No float, anywhere.
--   * Timestamps are TIMESTAMPTZ. Reporting still needs a per-restaurant
--     business day (a service crossing midnight is one day's takings), but that
--     is computed at read time from a UTC instant, never stored local.
--   * Ids are prefixed text so a stray id is identifiable on sight.
--   * Case-folded columns are CITEXT, never lower() functional indexes. One
--     mechanism, used everywhere: mixing them leaves some comparisons folding
--     case and some not, and the one that does not will be the login path.
--
-- Tenant isolation is enforced by the database, not by the application. Every
-- tenant-owned table below carries restaurant_id, ENABLE + FORCE ROW LEVEL
-- SECURITY, and a policy keyed on a transaction-local setting.
--
-- FORCE is not a hardening extra here, it is the whole mechanism: the app user
-- owns these tables, and an owner bypasses its own policies unless forced.
-- (Verified against Drop: the app role is neither superuser nor BYPASSRLS, and
-- CREATE ROLE is denied, so a separate unprivileged role is not available.)

CREATE EXTENSION IF NOT EXISTS citext;

-- ============================================================== registry ===
-- restaurants is the tenant list, not tenant-owned data, so it carries no
-- policy: resolving a restaurant by slug or webhook token has to work before
-- any tenant context exists. This is what removes most of the need for an
-- unscoped escape hatch rather than policing one.

CREATE TABLE restaurants (
  id                TEXT PRIMARY KEY,
  slug              CITEXT NOT NULL UNIQUE,
  name              TEXT NOT NULL,
  status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'active', 'suspended')),
  currency          TEXT NOT NULL DEFAULT 'ZAR',
  timezone          TEXT NOT NULL DEFAULT 'Africa/Johannesburg',
  -- Unguessable, ≥128 bits, and the path segment Paystack posts to. It is not
  -- an authenticator (the HMAC is); it selects whose key to verify against.
  webhook_token     TEXT NOT NULL UNIQUE,
  -- Explicit, never inferred from a missing key. A restaurant that finishes
  -- onboarding without working credentials must fail loudly, not take
  -- simulated payments that silently never arrive.
  mock_mode         BOOLEAN NOT NULL DEFAULT TRUE,
  -- Set only when a live secret key has passed a real connection test.
  live_enabled_at   TIMESTAMPTZ,
  -- Nullable on purpose: self-hosted installs and the bootstrap CLI both
  -- create restaurants with no platform account above them, and that is a
  -- supported state rather than an anomaly.
  created_by_platform_user_id TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  archived_at       TIMESTAMPTZ
);

-- ================================================================ staff ====

CREATE TABLE restaurant_users (
  id              TEXT PRIMARY KEY,
  restaurant_id   TEXT NOT NULL REFERENCES restaurants (id) ON DELETE CASCADE,
  email           CITEXT NOT NULL,
  display_name    TEXT,
  password_hash   TEXT NOT NULL,
  -- A permission string-set rather than a roles table. Custom role bundles
  -- arrive with a reader; until then a roles table would be one join with no
  -- caller -- and a nullable restaurant_id on it would punch a hole in exactly
  -- the join that decides permissions.
  role            TEXT NOT NULL DEFAULT 'viewer'
                    CHECK (role IN ('owner', 'manager', 'cashier', 'viewer')),
  permissions     JSONB NOT NULL DEFAULT '[]'::jsonb,
  totp_secret_enc TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at   TIMESTAMPTZ,
  disabled_at     TIMESTAMPTZ,
  UNIQUE (restaurant_id, id),
  UNIQUE (restaurant_id, email)
);

CREATE TABLE restaurant_sessions (
  id            TEXT PRIMARY KEY,
  restaurant_id TEXT NOT NULL,
  user_id       TEXT NOT NULL,
  -- Only the hash. A database dump does not hand anyone a live session.
  token_hash    TEXT NOT NULL UNIQUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at    TIMESTAMPTZ NOT NULL,
  ip            TEXT,
  user_agent    TEXT,
  revoked_at    TIMESTAMPTZ,
  FOREIGN KEY (restaurant_id, user_id)
    REFERENCES restaurant_users (restaurant_id, id) ON DELETE CASCADE
);

CREATE INDEX idx_rsessions_user ON restaurant_sessions (restaurant_id, user_id);
CREATE INDEX idx_rsessions_expiry ON restaurant_sessions (expires_at);

-- =============================================================== tables ====

CREATE TABLE tables (
  id            TEXT PRIMARY KEY,
  restaurant_id TEXT NOT NULL REFERENCES restaurants (id) ON DELETE CASCADE,
  -- Chosen by the admin, stable, printed on a sticker. Unique only within a
  -- restaurant, which is why the scan URL carries the slug.
  code          CITEXT NOT NULL,
  label         TEXT,
  seats         INTEGER NOT NULL DEFAULT 4 CHECK (seats > 0),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  archived_at   TIMESTAMPTZ,
  UNIQUE (restaurant_id, id),
  UNIQUE (restaurant_id, code)
);

-- A code that has been printed and then changed must never be handed to a
-- different table: an old sticker in the wild would route diners into another
-- party's bill, and in equal-split mode into their money.
CREATE TABLE retired_table_codes (
  restaurant_id TEXT NOT NULL REFERENCES restaurants (id) ON DELETE CASCADE,
  code          CITEXT NOT NULL,
  table_id      TEXT NOT NULL,
  retired_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (restaurant_id, code)
);

-- ============================================================= sessions ====
-- A seating is a bill. One row, not a session row plus a split row: two
-- lifecycles in a 1:1 relationship drift, and the one that drifts is the one
-- holding the money.

CREATE TABLE sessions (
  id                 TEXT PRIMARY KEY,
  restaurant_id      TEXT NOT NULL,
  table_id           TEXT NOT NULL,
  status             TEXT NOT NULL DEFAULT 'open'
                       CHECK (status IN ('open', 'locked', 'awaiting_payment',
                                         'paid', 'short', 'closed', 'abandoned')),
  pay_mode           TEXT CHECK (pay_mode IN ('equal', 'items', 'full')),
  -- The number of shares the bill divides into. Frozen at lock, and every
  -- share computes against this stored value rather than a live count -- a
  -- late scan must not change what an already-paid diner owed.
  headcount          INTEGER CHECK (headcount > 0),
  headcount_locked_at TIMESTAMPTZ,
  currency           TEXT NOT NULL DEFAULT 'ZAR',
  items_cents        BIGINT NOT NULL DEFAULT 0,
  service_cents      BIGINT NOT NULL DEFAULT 0,
  tip_cents          BIGINT NOT NULL DEFAULT 0,
  -- Printed on the slip for reference only. Never added to any total: menu
  -- prices here are VAT-inclusive and the slip's VAT line is a breakdown.
  vat_cents          BIGINT NOT NULL DEFAULT 0,
  -- Frozen when the mode is chosen, together with the tip, in one transaction.
  -- Null until then. Reconciliation is measured against this, not against a
  -- total recomputed from items on every read.
  bill_total_cents   BIGINT,
  bill_frozen_at     TIMESTAMPTZ,
  -- Shares with no participant to assign them to. Blocks 'paid'.
  unassigned_cents   BIGINT NOT NULL DEFAULT 0,
  confidence         TEXT,
  notes              TEXT,
  -- Staff own open and close; the idle timeout is only a backstop, and it must
  -- not fire while a checkout is live.
  opened_by_user_id  TEXT,
  cashier_user_id    TEXT,
  closed_at          TIMESTAMPTZ,
  closed_by_user_id  TEXT,
  short_cents        BIGINT,
  short_reason       TEXT,
  -- Optimistic concurrency: two staff acting at once must not be last-write-wins
  -- on a status that decides whether money is still owed.
  version            INTEGER NOT NULL DEFAULT 1,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_activity_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  archived_at        TIMESTAMPTZ,
  UNIQUE (restaurant_id, id),
  FOREIGN KEY (restaurant_id, table_id)
    REFERENCES tables (restaurant_id, id) ON DELETE RESTRICT
);

-- One open seating per table at a time. Without this, two diners scanning at
-- the same moment open two sessions and the party is split across two bills.
CREATE UNIQUE INDEX idx_sessions_one_open ON sessions (restaurant_id, table_id)
  WHERE status IN ('open', 'locked', 'awaiting_payment');

CREATE INDEX idx_sessions_created ON sessions (restaurant_id, created_at DESC);
CREATE INDEX idx_sessions_status ON sessions (restaurant_id, status);

CREATE TABLE session_participants (
  id            TEXT PRIMARY KEY,
  restaurant_id TEXT NOT NULL,
  session_id    TEXT NOT NULL,
  -- Nullable: people scan the sticker before they type a name. A NOT NULL name
  -- with a uniqueness constraint rejects the second nameless joiner, which is
  -- every table of more than one.
  name          TEXT,
  email         TEXT,
  is_host       BOOLEAN NOT NULL DEFAULT FALSE,
  -- SHA-256 of the bearer token handed out once at join. Identity is derived
  -- from this, never from an id in a request body.
  token_hash    TEXT,
  -- Server-minted and signed. Makes a rescan re-join instead of adding a
  -- phantom diner, which in equal mode would change what everyone else owes.
  device_hash   TEXT,
  seat_no       INTEGER,
  joined_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Ejection is a soft mark. A hard delete would cascade away payment rows,
  -- and money that actually reached the restaurant's account must never
  -- disappear from the ledger because someone tidied the roster.
  removed_at    TIMESTAMPTZ,
  UNIQUE (restaurant_id, id),
  FOREIGN KEY (restaurant_id, session_id)
    REFERENCES sessions (restaurant_id, id) ON DELETE CASCADE
);

-- Exactly one host. Two phones scanning within the same tick would otherwise
-- both read "no participants yet" and both insert as host -- and in full mode,
-- where only the host may raise a checkout, two hosts means two bills.
CREATE UNIQUE INDEX idx_participant_one_host ON session_participants (session_id)
  WHERE is_host AND removed_at IS NULL;

-- A rescan re-joins rather than double-counting. Enforced here rather than by
-- read-then-write, which races on flaky restaurant wifi.
CREATE UNIQUE INDEX idx_participant_device ON session_participants (session_id, device_hash)
  WHERE device_hash IS NOT NULL AND removed_at IS NULL;

-- Names are not credentials, but they must be distinguishable at one table.
-- Partial, so any number of not-yet-named diners can coexist.
CREATE UNIQUE INDEX idx_participant_name ON session_participants (session_id, lower(name))
  WHERE name IS NOT NULL AND removed_at IS NULL;

CREATE INDEX idx_participant_session ON session_participants (restaurant_id, session_id);
CREATE INDEX idx_participant_token ON session_participants (token_hash);

CREATE TABLE items (
  id            TEXT PRIMARY KEY,
  restaurant_id TEXT NOT NULL,
  session_id    TEXT NOT NULL,
  position      INTEGER NOT NULL,
  qty           INTEGER NOT NULL DEFAULT 1,
  description   TEXT NOT NULL,
  unit_cents    BIGINT NOT NULL DEFAULT 0,
  line_cents    BIGINT NOT NULL,
  UNIQUE (restaurant_id, session_id, id),
  FOREIGN KEY (restaurant_id, session_id)
    REFERENCES sessions (restaurant_id, id) ON DELETE CASCADE
);

CREATE INDEX idx_items_session ON items (restaurant_id, session_id, position);

CREATE TABLE claims (
  restaurant_id  TEXT NOT NULL,
  session_id     TEXT NOT NULL,
  item_id        TEXT NOT NULL,
  participant_id TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (item_id, participant_id),
  -- Three columns, not two. A (restaurant_id, item_id) reference would still
  -- permit a claim whose session is table 5 and whose item belongs to table 9
  -- of the same restaurant -- and the natural-language claim path inserts
  -- model-produced ids.
  FOREIGN KEY (restaurant_id, session_id, item_id)
    REFERENCES items (restaurant_id, session_id, id) ON DELETE CASCADE,
  FOREIGN KEY (restaurant_id, participant_id)
    REFERENCES session_participants (restaurant_id, id) ON DELETE CASCADE
);

CREATE INDEX idx_claims_session ON claims (restaurant_id, session_id);

-- ============================================================= payments ====

CREATE TABLE payments (
  id                TEXT PRIMARY KEY,
  restaurant_id     TEXT NOT NULL,
  session_id        TEXT NOT NULL,
  -- RESTRICT, not CASCADE. A payment row is the record that money arrived;
  -- removing a participant must never be able to delete it.
  participant_id    TEXT NOT NULL,
  -- Set when one diner covers another's share. Allowed in any mode and
  -- independent of the mode freeze: the last diner's phone dies and the host
  -- pays at the till, which is a nightly occurrence, not an edge case.
  on_behalf_of      TEXT,
  provider          TEXT NOT NULL DEFAULT 'paystack',
  -- Prefixed with the opaque restaurant id, so references cannot collide
  -- across tenants and the reference is itself a second tenant check.
  reference         TEXT NOT NULL,
  expected_cents    BIGINT NOT NULL,
  received_cents    BIGINT,
  currency          TEXT NOT NULL,
  status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'paid', 'underpaid', 'voided', 'failed')),
  authorization_url TEXT,
  provider_ref      TEXT,
  -- Set when the bill changed after this link was raised. The transaction stays
  -- payable at Paystack, so money arriving here is recorded and flagged rather
  -- than dropped or allowed to settle a share whose amount has moved.
  voided_at         TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  paid_at           TIMESTAMPTZ,
  UNIQUE (restaurant_id, id),
  UNIQUE (restaurant_id, reference),
  -- Whatever is recorded as paid must say how much actually arrived. The
  -- reconciliation invariant sums received_cents, and a NULL there would make
  -- it silently short.
  CONSTRAINT paid_has_amount CHECK (status <> 'paid' OR received_cents IS NOT NULL),
  FOREIGN KEY (restaurant_id, session_id)
    REFERENCES sessions (restaurant_id, id) ON DELETE CASCADE,
  FOREIGN KEY (restaurant_id, participant_id)
    REFERENCES session_participants (restaurant_id, id) ON DELETE RESTRICT
);

-- One live checkout per person per session. Two simultaneous taps on Pay would
-- otherwise both read "no live payment" and both insert -- and because the
-- reference carries a random suffix, the reference index would not catch it.
-- This has to be a database constraint: with an async driver the read-to-write
-- window cannot be closed in application code.
CREATE UNIQUE INDEX idx_payments_one_live ON payments (session_id, participant_id)
  WHERE status = 'pending' AND voided_at IS NULL;

CREATE INDEX idx_payments_session ON payments (restaurant_id, session_id);
CREATE INDEX idx_payments_status ON payments (restaurant_id, status, created_at DESC);
CREATE INDEX idx_payments_reference ON payments (reference);

-- ============================================================ telemetry ====

-- restaurant_id is NULLABLE here, deliberately. An inbound delivery on an
-- unknown webhook token has no tenant -- and "the operator pasted the wrong
-- URL into their Paystack dashboard" is precisely what this log exists to
-- diagnose. A NOT NULL column would make the commonest misconfiguration the
-- one event that cannot be recorded.
CREATE TABLE webhook_events (
  id              TEXT PRIMARY KEY,
  restaurant_id   TEXT REFERENCES restaurants (id) ON DELETE SET NULL,
  provider        TEXT NOT NULL,
  event_id        TEXT,
  event_type      TEXT,
  signature_valid BOOLEAN NOT NULL,
  status          TEXT NOT NULL,
  reference       TEXT,
  -- For unverified deliveries this holds a hash and a short prefix, not the
  -- body: an unauthenticated endpoint that stores 20KB per rejected request is
  -- a disk-exhaustion lever against every tenant at once.
  payload         TEXT,
  error           TEXT,
  received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at    TIMESTAMPTZ
);

-- Scoped per tenant. event_id comes from the tenant's own Paystack account, so
-- a global unique index means one restaurant's ids can suppress another's
-- genuine deliveries -- accidentally by collision, or deliberately, since a
-- tenant holds its own signing key and can mint valid events at will.
CREATE UNIQUE INDEX idx_webhook_dedupe ON webhook_events (restaurant_id, provider, event_id)
  WHERE event_id IS NOT NULL;
CREATE INDEX idx_webhook_received ON webhook_events (received_at DESC);

-- Telemetry outlives its subject: a single-column FK to restaurants and a
-- plain session_id with no FK. A composite FK with ON DELETE SET NULL nulls
-- every column of the key, which against a NOT NULL restaurant_id makes
-- deleting a session fail outright.
CREATE TABLE ai_usage (
  id                 TEXT PRIMARY KEY,
  restaurant_id      TEXT NOT NULL REFERENCES restaurants (id) ON DELETE CASCADE,
  session_id         TEXT,
  operation          TEXT NOT NULL,
  model              TEXT NOT NULL,
  input_tokens       BIGINT NOT NULL DEFAULT 0,
  output_tokens      BIGINT NOT NULL DEFAULT 0,
  cache_read_tokens  BIGINT NOT NULL DEFAULT 0,
  cache_write_tokens BIGINT NOT NULL DEFAULT 0,
  cost_micros        BIGINT NOT NULL DEFAULT 0,
  latency_ms         INTEGER NOT NULL DEFAULT 0,
  ok                 BOOLEAN NOT NULL DEFAULT TRUE,
  error              TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_ai_usage_created ON ai_usage (restaurant_id, created_at DESC);

CREATE TABLE audit_log (
  id            TEXT PRIMARY KEY,
  restaurant_id TEXT NOT NULL REFERENCES restaurants (id) ON DELETE CASCADE,
  session_id    TEXT,
  actor_type    TEXT NOT NULL CHECK (actor_type IN ('staff', 'participant', 'system', 'platform')),
  actor_id      TEXT,
  actor_label   TEXT,
  action        TEXT NOT NULL,
  target_type   TEXT,
  target_id     TEXT,
  meta          JSONB,
  ip            TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_created ON audit_log (restaurant_id, created_at DESC);

-- =========================================================== settings ======

CREATE TABLE restaurant_settings (
  restaurant_id TEXT NOT NULL REFERENCES restaurants (id) ON DELETE CASCADE,
  key           TEXT NOT NULL,
  value         TEXT NOT NULL,
  is_secret     BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by    TEXT,
  PRIMARY KEY (restaurant_id, key)
);

-- Platform-wide, not tenant-owned: no policy, same reasoning as restaurants.
CREATE TABLE platform_settings (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  is_secret  BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by TEXT
);

-- ================================================== row level security =====
--
-- app.restaurant_id is set transaction-locally by the request middleware. When
-- it is unset, current_setting(..., true) is NULL, the comparison is NULL, the
-- policy is not satisfied and the query returns nothing -- including
-- aggregates, which is the leak an application-layer scoping check cannot see.
--
-- app.platform_mode is the deliberate escape hatch. It exists for two flows
-- only (cross-tenant platform reporting, and reading webhook deliveries that
-- have no tenant) and is set in exactly one module. A GUC is weaker than the
-- separate BYPASSRLS role this would otherwise use; that role is unavailable
-- because the platform denies CREATE ROLE. Recorded as a known downgrade.

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'restaurant_users', 'restaurant_sessions', 'tables', 'retired_table_codes',
    'sessions', 'session_participants', 'items', 'claims', 'payments',
    'ai_usage', 'audit_log', 'restaurant_settings'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE  ROW LEVEL SECURITY', t);
    EXECUTE format($f$
      CREATE POLICY tenant_isolation ON %I
        USING      (restaurant_id = current_setting('app.restaurant_id', true)
                    OR current_setting('app.platform_mode', true) = 'on')
        WITH CHECK (restaurant_id = current_setting('app.restaurant_id', true))
    $f$, t);
  END LOOP;
END $$;

-- webhook_events is the one tenant table whose restaurant_id may be NULL, so
-- its policy is written by hand: a tenant sees only its own rows, and the
-- unroutable ones are visible on the platform plane.
ALTER TABLE webhook_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_events FORCE  ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON webhook_events
  USING      (restaurant_id = current_setting('app.restaurant_id', true)
              OR current_setting('app.platform_mode', true) = 'on')
  WITH CHECK (restaurant_id = current_setting('app.restaurant_id', true)
              OR current_setting('app.platform_mode', true) = 'on');
