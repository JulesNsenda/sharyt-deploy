-- Link identity, and how long a diner's access to a table lasts.
--
-- Two problems, both of which end with somebody paying the wrong restaurant or
-- reading a stranger's bill.

------------------------------------------------------- slugs are forever ----
--
-- Slugs are unique, so two live restaurants cannot share a link. What was
-- missing is that they were also *reusable*: restaurant A renames itself,
-- restaurant B claims the freed handle, and every QR sticker A has already
-- printed now routes diners into B's tables -- to pay B for food they ate at A.
--
-- Table codes were already retired for exactly this reason. Slugs were not.

CREATE TABLE retired_slugs (
  slug          CITEXT PRIMARY KEY,
  restaurant_id TEXT NOT NULL REFERENCES restaurants (id) ON DELETE CASCADE,
  retired_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every existing slug is claimed by its owner, so nothing already printed can
-- be taken by a later signup.
INSERT INTO retired_slugs (slug, restaurant_id)
SELECT slug, id FROM restaurants
ON CONFLICT DO NOTHING;

-- The disambiguator appended to the handle a restaurant asks for. Stored rather
-- than derived: it has to survive a rename of the readable part, and deriving
-- it from anything mutable would change the link.
ALTER TABLE restaurants ADD COLUMN slug_discriminator TEXT;

--------------------------------------------- a diner's access has an end ----
--
-- The QR sticker on a table is permanent, so tonight's diners scan exactly the
-- same code as last night's. What must not be permanent is anyone's access to
-- what they find there.
--
-- Two independent limits, because they fail differently. The token expires, so
-- a phone left on a bus cannot act on a table hours later. And reading a
-- finished session requires having been *in* it, so the next party to sit down
-- cannot see the previous party's bill, their names, or what they paid.

ALTER TABLE session_participants
  ADD COLUMN token_expires_at TIMESTAMPTZ,
  -- Refreshed on activity. A table that has gone quiet for hours is over,
  -- whatever its status column says.
  ADD COLUMN last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Existing rows get a window from now rather than instantly expiring, so an
-- in-flight table is not cut off by a deployment.
UPDATE session_participants SET token_expires_at = now() + interval '4 hours'
  WHERE token_expires_at IS NULL AND removed_at IS NULL;

CREATE INDEX idx_participant_expiry ON session_participants (token_expires_at)
  WHERE removed_at IS NULL;

-- How long after a table closes its diners can still pull up their receipt.
-- Short, because the value decays fast and the risk does not: the next party is
-- usually seated within the hour.
ALTER TABLE sessions ADD COLUMN receipt_until TIMESTAMPTZ;
