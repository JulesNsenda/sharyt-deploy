-- Account lifecycle, and the platform plane above the tenants.

------------------------------------------------------- account lifecycle ----

ALTER TABLE restaurant_users
  -- Nobody signs in on an address they have not proved they can read. Without
  -- this, a typo in the signup form silently creates an account whose owner can
  -- never recover it, and a deliberate typo creates one in somebody else's name.
  ADD COLUMN email_verified_at TIMESTAMPTZ,
  -- Set on accounts an administrator created, because the administrator chose
  -- the password and therefore knows it.
  ADD COLUMN must_change_password BOOLEAN NOT NULL DEFAULT FALSE,
  -- Two-factor is offered from the first sign-in and can be declined. This
  -- counts the declines: after enough of them it stops being a suggestion.
  -- A counter rather than a date, because "seven logins" is what was asked for
  -- and it tracks actual use rather than elapsed time on an unused account.
  ADD COLUMN logins_without_mfa INTEGER NOT NULL DEFAULT 0;

-- Existing accounts predate verification. Treating them as unverified would
-- lock out the people already using the product to enforce a rule they were
-- never given a chance to satisfy.
UPDATE restaurant_users SET email_verified_at = created_at WHERE email_verified_at IS NULL;

/*
 * One-time links: verify an address, reset a password.
 *
 * Only the hash is stored, exactly like a session token -- a database dump must
 * not hand anyone a working password-reset link. Single use, short lived, and
 * consumed atomically so a link forwarded to two devices works once.
 */
CREATE TABLE auth_tokens (
  id            TEXT PRIMARY KEY,
  restaurant_id TEXT REFERENCES restaurants (id) ON DELETE CASCADE,
  user_id       TEXT NOT NULL,
  purpose       TEXT NOT NULL CHECK (purpose IN ('verify_email', 'reset_password')),
  token_hash    TEXT NOT NULL UNIQUE,
  expires_at    TIMESTAMPTZ NOT NULL,
  consumed_at   TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_auth_tokens_user ON auth_tokens (user_id, purpose);
CREATE INDEX idx_auth_tokens_expiry ON auth_tokens (expires_at);

-- No tenant policy. A verification link is followed by somebody who is not
-- signed in and has no tenant context yet -- that is the entire point of it.
-- The token is the credential; it is unguessable and single-use.

--------------------------------------------------------- the platform -------
--
-- The third identity plane. Separate table, separate cookie, separate app --
-- not a role on restaurant_users. A nullable tenant column on a user table is
-- an invitation to the bug where a scoping predicate bound to NULL matches
-- nothing, or with the wrong SQL, everything.

CREATE TABLE platform_users (
  id              TEXT PRIMARY KEY,
  email           CITEXT NOT NULL UNIQUE,
  display_name    TEXT,
  password_hash   TEXT NOT NULL,
  totp_secret_enc TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at   TIMESTAMPTZ,
  disabled_at     TIMESTAMPTZ
);

CREATE TABLE platform_sessions (
  id           TEXT PRIMARY KEY,
  user_id      TEXT NOT NULL REFERENCES platform_users (id) ON DELETE CASCADE,
  token_hash   TEXT NOT NULL UNIQUE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at   TIMESTAMPTZ NOT NULL,
  ip           TEXT,
  user_agent   TEXT,
  revoked_at   TIMESTAMPTZ
);

CREATE INDEX idx_platform_sessions_user ON platform_sessions (user_id);

------------------------------------------------------------ subscriptions ---
--
-- Kept on `restaurants` rather than in its own table: there is one current
-- subscription per restaurant and no billing history to model yet. When there
-- is, it becomes a table and this becomes a denormalised pointer to the latest
-- row -- which is a migration, not a redesign.

ALTER TABLE restaurants
  ADD COLUMN plan TEXT NOT NULL DEFAULT 'trial'
    CHECK (plan IN ('trial', 'starter', 'standard', 'unlimited')),
  ADD COLUMN plan_status TEXT NOT NULL DEFAULT 'active'
    CHECK (plan_status IN ('active', 'past_due', 'cancelled')),
  ADD COLUMN trial_ends_at TIMESTAMPTZ,
  -- Free text, written by whoever is looking after the account.
  ADD COLUMN platform_notes TEXT,
  -- Set when the platform suspends a venue. Distinct from `status`, which the
  -- restaurant's own lifecycle owns: a suspension is done *to* them and must
  -- not be clearable by them.
  ADD COLUMN suspended_at TIMESTAMPTZ,
  ADD COLUMN suspended_reason TEXT;
