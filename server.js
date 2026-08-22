import { createRequire as __cr } from 'node:module';
const require = __cr(import.meta.url);

// apps/server/src/app.ts
import express2 from "express";
import cookieParser from "cookie-parser";
import path3 from "node:path";
import { existsSync as existsSync3 } from "node:fs";
import { fileURLToPath as fileURLToPath2 } from "node:url";

// apps/server/src/db/index.ts
import pg from "pg";
import { readFileSync, readdirSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
var { Pool } = pg;
var here = path.dirname(fileURLToPath(import.meta.url));
pg.types.setTypeParser(pg.types.builtins.INT8, (v) => Number(v));
pg.types.setTypeParser(pg.types.builtins.NUMERIC, (v) => v);
function makeQuery(client) {
  const q = (async (text, params = []) => {
    const res = await client.query(text, params);
    return res.rows;
  });
  q.one = async (text, params = []) => {
    const rows = await q(text, params);
    if (rows.length > 1) throw new Error(`Expected at most one row, got ${rows.length}`);
    return rows[0] ?? null;
  };
  return q;
}
var Database = class {
  pool;
  constructor(opts) {
    this.pool = new Pool({
      connectionString: opts.connectionString,
      max: opts.max ?? 10,
      idleTimeoutMillis: 3e4,
      connectionTimeoutMillis: 1e4,
      // A runaway query must not hold one of a small number of connections
      // open indefinitely. Every request needs one, so exhaustion is an outage.
      statement_timeout: opts.statementTimeoutMs ?? 15e3
    });
    this.pool.on("error", (err) => console.error("[db] idle client error:", err.message));
  }
  /**
   * Run inside a transaction with the tenant pinned for its duration.
   *
   * `set_config(..., true)` is transaction-local. A session-level `SET` would
   * outlive the request on a pooled connection and leak the tenant to whoever
   * checked that connection out next -- which is the whole failure this design
   * exists to prevent, so it is worth being explicit about.
   *
   * Everything the request touches must run through the handle passed to `fn`.
   */
  async withTenant(restaurantId, fn) {
    return this.transaction(async (client) => {
      await client.query("SELECT set_config($1, $2, true)", ["app.restaurant_id", restaurantId]);
      return fn(makeQuery(client));
    });
  }
  /**
   * The unscoped escape hatch, and the only one.
   *
   * Row-level security is the isolation mechanism, so a query that runs without
   * tenant context sees nothing at all -- including aggregates. That is correct
   * for tenant data and wrong for the handful of flows that must span tenants
   * or precede them: resolving a restaurant by slug or webhook token, the
   * platform plane, and reading webhook deliveries that never had a tenant.
   *
   * Its callers are enumerated in `unscoped.ts` and asserted by a test. It is a
   * weaker boundary than a separate BYPASSRLS role would be; that role is not
   * available, because the platform denies CREATE ROLE.
   */
  async withPlatform(fn) {
    return this.transaction(async (client) => {
      await client.query("SELECT set_config($1, $2, true)", ["app.platform_mode", "on"]);
      return fn(makeQuery(client));
    });
  }
  /**
   * No tenant, no platform flag. Sees only tables that carry no policy at all:
   * `restaurants` and `platform_settings`. Every tenant table is invisible
   * here, which is the point -- it is the safe default, not a privileged one.
   */
  async withRegistry(fn) {
    return this.transaction(async (client) => fn(makeQuery(client)));
  }
  async transaction(fn) {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const out = await fn(client);
      await client.query("COMMIT");
      return out;
    } catch (err) {
      try {
        await client.query("ROLLBACK");
      } catch {
      }
      throw err;
    } finally {
      client.release();
    }
  }
  /**
   * Numbered SQL files, applied in filename order, recorded in
   * schema_migrations. No framework: a person reading this repo can see exactly
   * what the schema is and how it got there.
   *
   * Postgres DDL is transactional, so a failed migration leaves nothing behind.
   * The advisory lock is what makes two instances booting at once safe -- both
   * would otherwise read "not yet applied" and both would apply.
   */
  async migrate() {
    const client = await this.pool.connect();
    try {
      await client.query("SELECT pg_advisory_lock($1)", [MIGRATION_LOCK]);
      await client.query(`
        CREATE TABLE IF NOT EXISTS schema_migrations (
          name       TEXT PRIMARY KEY,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);
      const applied = new Set(
        (await client.query("SELECT name FROM schema_migrations")).rows.map((r) => r.name)
      );
      const ran = [];
      for (const file of migrationFiles()) {
        if (applied.has(file)) continue;
        const sql = readFileSync(path.join(migrationsDir(), file), "utf8");
        await client.query("BEGIN");
        try {
          await client.query(sql);
          await client.query("INSERT INTO schema_migrations (name) VALUES ($1)", [file]);
          await client.query("COMMIT");
        } catch (err) {
          await client.query("ROLLBACK");
          throw new Error(`Migration ${file} failed: ${err.message}`);
        }
        ran.push(file);
      }
      return ran;
    } finally {
      await client.query("SELECT pg_advisory_unlock($1)", [MIGRATION_LOCK]).catch(() => void 0);
      client.release();
    }
  }
  /** Tests only: drop everything and re-migrate. */
  async reset() {
    await this.pool.query("DROP SCHEMA public CASCADE; CREATE SCHEMA public;");
    await this.migrate();
  }
  async close() {
    await this.pool.end();
  }
};
var MIGRATION_LOCK = 8675309;
function migrationsDir() {
  for (const candidate of [
    path.join(here, "migrations"),
    path.resolve(here, "../../src/db/migrations"),
    path.resolve(here, "db/migrations")
  ]) {
    if (existsSync(candidate)) return candidate;
  }
  throw new Error("Could not find the migrations directory.");
}
function migrationFiles() {
  return readdirSync(migrationsDir()).filter((f) => f.endsWith(".sql")).sort();
}
var iso = (v) => v instanceof Date ? v.toISOString() : typeof v === "string" ? v : null;
var isoRequired = (v) => iso(v) ?? (/* @__PURE__ */ new Date(0)).toISOString();

// apps/server/src/db/registry.ts
import { randomBytes } from "node:crypto";

// apps/server/src/services/crypto.ts
import crypto from "node:crypto";
import { readFileSync as readFileSync2, writeFileSync, existsSync as existsSync2, mkdirSync, chmodSync } from "node:fs";
import path2 from "node:path";
var CryptoError = class extends Error {
};
function resolveAppSecret(dataDir, env = process.env) {
  const fromEnv = env.APP_SECRET?.trim();
  if (fromEnv) return { key: deriveKey(fromEnv), source: "env" };
  const file = path2.join(dataDir, ".app_secret");
  try {
    if (existsSync2(file)) {
      const raw = readFileSync2(file, "utf8").trim();
      if (!raw) {
        throw new CryptoError(
          `${file} exists but is empty. Delete it to generate a new key, or restore it from backup.`
        );
      }
      return { key: deriveKey(raw), source: "file" };
    }
    if (!existsSync2(dataDir)) mkdirSync(dataDir, { recursive: true });
    const generated = crypto.randomBytes(32).toString("base64");
    writeFileSync(file, `${generated}
`, { encoding: "utf8", mode: 384 });
    try {
      chmodSync(file, 384);
    } catch {
    }
    return { key: deriveKey(generated), source: "file" };
  } catch (err) {
    if (err instanceof CryptoError) throw err;
    return null;
  }
}
function appSecretFrom(raw) {
  return deriveKey(raw);
}
function newAppSecret() {
  return crypto.randomBytes(32).toString("base64");
}
function deriveKey(secret) {
  return crypto.createHash("sha256").update(secret, "utf8").digest();
}
var ENVELOPE_VERSION = 1;
function encryptSecret(plaintext, key) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const ct = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return JSON.stringify({
    v: ENVELOPE_VERSION,
    iv: iv.toString("base64"),
    tag: tag.toString("base64"),
    ct: ct.toString("base64")
  });
}
function decryptSecret(envelope, key) {
  let parsed;
  try {
    parsed = JSON.parse(envelope);
  } catch {
    throw new CryptoError("Stored credential is not a valid envelope.");
  }
  if (parsed.v !== ENVELOPE_VERSION || !parsed.iv || !parsed.tag || !parsed.ct) {
    throw new CryptoError(`Unsupported credential envelope (version ${String(parsed.v)}).`);
  }
  const decipher = crypto.createDecipheriv("aes-256-gcm", key, Buffer.from(parsed.iv, "base64"));
  decipher.setAuthTag(Buffer.from(parsed.tag, "base64"));
  try {
    return Buffer.concat([decipher.update(Buffer.from(parsed.ct, "base64")), decipher.final()]).toString("utf8");
  } catch {
    throw new CryptoError("Could not decrypt a stored credential with the current app secret.");
  }
}
function mintToken() {
  return crypto.randomBytes(32).toString("base64url");
}
function hashToken(token) {
  return crypto.createHash("sha256").update(String(token)).digest("hex");
}
var SCRYPT_N = 32768;
var SCRYPT_R = 8;
var SCRYPT_P = 1;
var SCRYPT_KEYLEN = 64;
var SCRYPT_MAXMEM = 192 * 1024 * 1024;
function hashPassword(password) {
  const salt = crypto.randomBytes(16);
  const hash2 = crypto.scryptSync(password.normalize("NFKC"), salt, SCRYPT_KEYLEN, {
    N: SCRYPT_N,
    r: SCRYPT_R,
    p: SCRYPT_P,
    maxmem: SCRYPT_MAXMEM
  });
  return `scrypt$${SCRYPT_N}$${SCRYPT_R}$${SCRYPT_P}$${salt.toString("base64")}$${hash2.toString("base64")}`;
}
function verifyPassword(password, stored) {
  const parts = stored.split("$");
  if (parts.length !== 6 || parts[0] !== "scrypt") return false;
  const N = Number(parts[1]);
  const r = Number(parts[2]);
  const p = Number(parts[3]);
  if (!Number.isFinite(N) || !Number.isFinite(r) || !Number.isFinite(p)) return false;
  const salt = Buffer.from(parts[4], "base64");
  const expected = Buffer.from(parts[5], "base64");
  let actual;
  try {
    actual = crypto.scryptSync(password.normalize("NFKC"), salt, expected.length, {
      N,
      r,
      p,
      maxmem: SCRYPT_MAXMEM
    });
  } catch {
    return false;
  }
  return actual.length === expected.length && crypto.timingSafeEqual(actual, expected);
}
function newId(prefix) {
  return `${prefix}_${crypto.randomBytes(9).toString("base64url")}`;
}

// apps/server/src/db/registry.ts
function map(r) {
  return {
    id: r.id,
    slug: r.slug,
    name: r.name,
    status: r.status,
    currency: r.currency,
    timezone: r.timezone,
    webhookToken: r.webhook_token,
    mockMode: r.mock_mode,
    liveEnabledAt: iso(r.live_enabled_at),
    createdAt: isoRequired(r.created_at),
    archivedAt: iso(r.archived_at)
  };
}
var COLUMNS = `id, slug, name, status, currency, timezone, webhook_token,
                 mock_mode, live_enabled_at, created_at, archived_at`;
var RESERVED_SLUGS = /* @__PURE__ */ new Set([
  "api",
  "admin",
  "platform",
  "mock",
  "assets",
  "static",
  "public",
  "t",
  "r",
  "health",
  "login",
  "logout",
  "signup",
  "sharyt",
  "sentinel",
  "pay",
  "www",
  "app",
  "cdn",
  "favicon.ico",
  "robots.txt",
  "well-known",
  ".well-known"
]);
var SLUG_PATTERN = /^[a-z0-9](?:[a-z0-9-]{1,38}[a-z0-9])$/;
var DISCRIMINATOR_ALPHABET = "bcdfghjkmnpqrstvwxz23456789";
function newDiscriminator(length = 4) {
  const bytes = randomBytes(length);
  let out = "";
  for (let i = 0; i < length; i++) {
    out += DISCRIMINATOR_ALPHABET[bytes[i] % DISCRIMINATOR_ALPHABET.length];
  }
  return out;
}
function slugProblem(raw) {
  const slug = raw.normalize("NFKC").trim().toLowerCase();
  if (!SLUG_PATTERN.test(slug)) {
    return "Use 3 to 40 characters: lowercase letters, numbers and hyphens, not starting or ending with a hyphen.";
  }
  if (RESERVED_SLUGS.has(slug)) return "That name is reserved. Pick another.";
  return null;
}
var RegistryRepository = class {
  constructor(db) {
    this.db = db;
  }
  async bySlug(slug) {
    const row = await this.db.withRegistry(
      (q) => q.one(`SELECT ${COLUMNS} FROM restaurants WHERE slug = $1 AND archived_at IS NULL`, [
        slug.normalize("NFKC").trim()
      ])
    );
    return row ? map(row) : null;
  }
  async byId(id) {
    const row = await this.db.withRegistry(
      (q) => q.one(`SELECT ${COLUMNS} FROM restaurants WHERE id = $1`, [id])
    );
    return row ? map(row) : null;
  }
  /**
   * Resolves the tenant a Paystack delivery claims to be for. The token selects
   * whose signing key to verify against; it authenticates nothing on its own,
   * and the caller must still check the HMAC and then confirm the payment it
   * resolves actually belongs to this restaurant.
   */
  async byWebhookToken(token) {
    const row = await this.db.withRegistry(
      (q) => q.one(`SELECT ${COLUMNS} FROM restaurants WHERE webhook_token = $1`, [token])
    );
    return row ? map(row) : null;
  }
  async list() {
    const rows = await this.db.withRegistry(
      (q) => q(`SELECT ${COLUMNS} FROM restaurants ORDER BY created_at DESC`)
    );
    return rows.map(map);
  }
  async count() {
    const row = await this.db.withRegistry(
      (q) => q.one("SELECT count(*)::int AS n FROM restaurants")
    );
    return row?.n ?? 0;
  }
  /**
   * Create a restaurant, and mint the link nobody else can ever hold.
   *
   * The handle the operator asks for is only the readable half. A random
   * discriminator is appended, so two venues with the same name get different
   * links without either having to compromise on their name -- and the result
   * is recorded in `retired_slugs` immediately, because a slug that has ever
   * existed must never be handed to anybody else.
   */
  async create(input) {
    const base = input.slug.normalize("NFKC").trim().toLowerCase();
    const problem = slugProblem(base);
    if (problem) throw new SlugRejected(problem);
    const id = newId("r");
    for (let attempt = 0; attempt < 8; attempt++) {
      const discriminator = newDiscriminator();
      const slug = `${base}-${discriminator}`;
      try {
        const row = await this.db.withRegistry(async (q) => {
          const created = await q.one(
            `INSERT INTO restaurants (id, slug, slug_discriminator, name, currency, timezone,
                                      webhook_token, created_by_platform_user_id)
             VALUES ($1, $2, $3, $4, coalesce($5, 'ZAR'), coalesce($6, 'Africa/Johannesburg'), $7, $8)
             RETURNING ${COLUMNS}`,
            [
              id,
              slug,
              discriminator,
              input.name.trim(),
              input.currency ?? null,
              input.timezone ?? null,
              // 256 bits. Long enough that enumeration is not a strategy, and
              // rotatable, because it ends up in access and proxy logs.
              `wht_${mintToken()}${mintToken()}`,
              input.createdByPlatformUserId ?? null
            ]
          );
          await q("INSERT INTO retired_slugs (slug, restaurant_id) VALUES ($1, $2)", [slug, id]);
          return created;
        });
        return map(row);
      } catch (err) {
        const message = String(err.message);
        const collided = message.includes("retired_slugs_pkey") || message.includes("restaurants_slug_key");
        if (!collided) throw err;
      }
    }
    throw new SlugRejected("Could not allocate a link for that name. Try a slightly different one.");
  }
  /**
   * Rename the readable half of a link.
   *
   * The old slug is retired, never freed. Otherwise every sticker already on
   * that restaurant's tables becomes a route into whoever claims the handle
   * next -- diners paying a stranger for food they ate somewhere else.
   */
  async changeSlug(restaurantId, newBase) {
    const base = newBase.normalize("NFKC").trim().toLowerCase();
    const problem = slugProblem(base);
    if (problem) throw new SlugRejected(problem);
    return this.db.withRegistry(async (q) => {
      const current = await q.one(
        "SELECT slug, slug_discriminator FROM restaurants WHERE id = $1",
        [restaurantId]
      );
      if (!current) throw new SlugRejected("No such restaurant.");
      const discriminator = current.slug_discriminator ?? newDiscriminator();
      const slug = `${base}-${discriminator}`;
      const taken = await q.one("SELECT slug FROM retired_slugs WHERE slug = $1", [slug]);
      if (taken && taken.slug !== current.slug) {
        throw new SlugRejected("That link has been used before and cannot be reissued.");
      }
      await q("INSERT INTO retired_slugs (slug, restaurant_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", [
        slug,
        restaurantId
      ]);
      const row = await q.one(
        `UPDATE restaurants SET slug = $2, slug_discriminator = $3 WHERE id = $1 RETURNING ${COLUMNS}`,
        [restaurantId, slug, discriminator]
      );
      return map(row);
    });
  }
  /**
   * Live payments are unlocked by evidence, not by a checkbox: the caller must
   * have proved a *live* secret key answers. A test key passes an authenticated
   * call just as well, which is exactly how a restaurant ends up taking
   * simulated payments that never settle.
   */
  async enableLive(restaurantId) {
    await this.db.withRegistry(
      (q) => q("UPDATE restaurants SET mock_mode = FALSE, live_enabled_at = now(), status = $2 WHERE id = $1", [
        restaurantId,
        "active"
      ])
    );
  }
  async setMockMode(restaurantId, mock2) {
    await this.db.withRegistry((q) => q("UPDATE restaurants SET mock_mode = $2 WHERE id = $1", [restaurantId, mock2]));
  }
};
var SlugRejected = class extends Error {
};

// apps/server/src/routes/table.ts
import { Router } from "express";

// packages/shared/dist/money.js
var MoneyError = class extends Error {
};
function assertCents(value, label) {
  if (!Number.isInteger(value) || value < 0) {
    throw new MoneyError(`${label} must be a non-negative integer number of cents, got ${value}`);
  }
}
function divideCents(total, n, offset = 0) {
  assertCents(total, "total");
  if (n <= 0)
    return [];
  const base = Math.floor(total / n);
  let remainder = total - base * n;
  const parts = new Array(n).fill(base);
  for (let i = 0; remainder > 0; i++, remainder--) {
    const target = ((i + offset) % n + n) % n;
    parts[target] = parts[target] + 1;
  }
  return parts;
}
var formatterCache = /* @__PURE__ */ new Map();
function formatMoney(cents, currency = "ZAR", locale = "en-ZA") {
  const key = `${locale}|${currency}`;
  let formatter = formatterCache.get(key);
  if (!formatter) {
    try {
      formatter = new Intl.NumberFormat(locale, {
        style: "currency",
        currency,
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      });
    } catch {
      formatter = new Intl.NumberFormat(locale, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      });
    }
    formatterCache.set(key, formatter);
  }
  return formatter.format(cents / 100);
}
function parseAmountToCents(input) {
  if (typeof input === "number") {
    return Number.isFinite(input) ? Math.round(input * 100) : 0;
  }
  if (!input)
    return 0;
  let s = String(input).replace(/[^\d.,-]/g, "");
  const lastComma = s.lastIndexOf(",");
  const lastDot = s.lastIndexOf(".");
  if (lastComma > lastDot) {
    s = s.replace(/\./g, "").replace(",", ".");
  } else {
    s = s.replace(/,/g, "");
  }
  const n = Number.parseFloat(s);
  return Number.isFinite(n) ? Math.round(n * 100) : 0;
}

// packages/shared/dist/types.js
var PAY_MODES = ["equal", "items", "full"];
var STAFF_ROLES = ["owner", "manager", "cashier", "viewer"];

// packages/shared/dist/permissions.js
var PERMISSIONS = {
  owner: ["*"],
  manager: [
    "table.manage",
    "session.open",
    "session.close",
    "session.close_short",
    "session.amend_bill",
    "payment.verify",
    "payment.void",
    "report.read",
    "receipt.print",
    "staff.manage",
    "settings.manage"
  ],
  cashier: [
    "session.open",
    "session.close",
    "session.amend_bill",
    "payment.verify",
    "receipt.print"
  ],
  viewer: ["report.read"]
};
function can(role, permission) {
  const granted = PERMISSIONS[role];
  return granted.includes("*") || granted.includes(permission);
}

// packages/shared/dist/schemas.js
import { z } from "zod";
var tableCodeSchema = z.string().trim().min(1, "Give the table a code.").max(16, "Keep table codes short enough to print.").regex(/^[A-Za-z0-9][A-Za-z0-9-]*$/, "Letters, numbers and hyphens only.");
var slugSchema = z.string().trim().toLowerCase().min(3).max(40).regex(/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])$/, "Lowercase letters, numbers and hyphens.");
var emailSchema = z.string().trim().email().max(200);
var passwordSchema = z.string().min(12, "Use at least 12 characters -- length beats punctuation.").max(200);
var centsSchema = z.number().int().min(0).max(1e8);
var joinSessionSchema = z.object({
  /** Optional: people scan first and introduce themselves later, if at all. */
  name: z.string().trim().min(1).max(40).optional()
});
var setNameSchema = z.object({
  name: z.string().trim().min(1).max(40)
});
var lockRosterSchema = z.object({
  /**
   * May exceed the number of devices -- two people share a phone constantly --
   * but never fall below it. The server rejects that rather than leaving shares
   * nobody can be charged for.
   */
  headcount: z.number().int().min(1).max(50),
  payMode: z.enum(PAY_MODES),
  tipCents: centsSchema.default(0),
  serviceCents: centsSchema.default(0),
  /** Optimistic concurrency: two staff or a staff member and the host. */
  version: z.number().int().optional()
});
var claimSchema = z.object({
  itemId: z.string().min(1),
  claimed: z.boolean(),
  /**
   * Claiming on someone else's behalf raises *their* share, so it needs their
   * agreement rather than just a field in a request body.
   */
  forParticipantId: z.string().min(1).optional()
});
var ejectSchema = z.object({ participantId: z.string().min(1) });
var transferHostSchema = z.object({ participantId: z.string().min(1) });
var checkoutSchema = z.object({
  /**
   * Covering somebody else's share. Allowed in any mode and independent of the
   * mode freeze -- the last diner's phone dies and the host pays at the till,
   * which happens nightly.
   */
  onBehalfOf: z.string().min(1).optional(),
  /**
   * Covering the shares that belong to nobody.
   *
   * Distinct from `onBehalfOf`, which names a real participant. When headcount
   * exceeds the roster -- two people to one phone -- the surplus shares have no
   * id to point at, and overloading `onBehalfOf` with a sentinel would make an
   * unrecognised id silently mean "charge me the remainder".
   */
  coverUnassigned: z.boolean().optional()
});
var billItemSchema = z.object({
  id: z.string().min(1).optional(),
  qty: z.number().int().min(1).max(99).default(1),
  description: z.string().trim().min(1).max(80),
  lineCents: centsSchema
});
var updateBillSchema = z.object({
  items: z.array(billItemSchema).max(120),
  serviceCents: centsSchema.optional(),
  tipCents: centsSchema.optional(),
  vatCents: centsSchema.optional()
});
var loginIdentifierSchema = z.string().trim().min(3).max(200).regex(/^[^\s@]+(?:@[^\s@]+\.[^\s@]+)?$/, "Use your email address, or your username.");
var staffLoginSchema = z.object({
  slug: slugSchema,
  email: loginIdentifierSchema,
  password: z.string().min(1).max(200),
  totp: z.string().trim().regex(/^\d{6}$/).optional()
});
var createStaffSchema = z.object({
  email: emailSchema,
  password: passwordSchema,
  displayName: z.string().trim().max(80).optional(),
  role: z.enum(STAFF_ROLES)
});
var createTableSchema = z.object({
  code: tableCodeSchema,
  label: z.string().trim().max(60).optional(),
  seats: z.number().int().min(1).max(40).default(4)
});
var updateTableSchema = createTableSchema.partial();
var openSessionSchema = z.object({
  tableId: z.string().min(1)
});
var closeShortSchema = z.object({
  /** Required. Writing down the restaurant's own money needs a stated reason. */
  reason: z.string().trim().min(3).max(200),
  version: z.number().int().optional()
});
var signupSchema = z.object({
  slug: slugSchema,
  name: z.string().trim().min(2).max(80),
  email: emailSchema,
  password: passwordSchema,
  currency: z.string().trim().length(3).toUpperCase().default("ZAR")
});
var requestResetSchema = z.object({ slug: slugSchema, email: emailSchema });
var completeResetSchema = z.object({
  token: z.string().min(10),
  password: passwordSchema
});
var changePasswordSchema = z.object({
  currentPassword: z.string().min(1).max(200),
  newPassword: passwordSchema
});
var enableMfaSchema = z.object({
  secret: z.string().min(16).max(64),
  code: z.string().trim().regex(/^\d{6}$/, "Six digits from your authenticator app.")
});
var platformLoginSchema = z.object({
  email: emailSchema,
  password: z.string().min(1).max(200)
});
var smtpSettingsSchema = z.object({
  host: z.string().trim().max(200).optional(),
  port: z.string().trim().max(6).optional(),
  secure: z.string().trim().optional(),
  user: z.string().trim().max(200).optional(),
  password: z.string().max(400).optional(),
  fromName: z.string().trim().max(80).optional(),
  fromAddress: z.string().trim().max(200).optional()
});
var subscriptionSchema = z.object({
  plan: z.enum(["trial", "starter", "standard", "unlimited"]).optional(),
  planStatus: z.enum(["active", "past_due", "cancelled"]).optional(),
  notes: z.string().trim().max(2e3).optional()
});
var suspendSchema = z.object({
  suspended: z.boolean(),
  reason: z.string().trim().max(300).optional()
});
var BILL_SCAN_JSON_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["venue", "table", "items", "subtotal", "service", "vat", "total", "confidence", "notes"],
  properties: {
    venue: { type: ["string", "null"], description: "Restaurant name printed at the top" },
    table: { type: ["string", "null"], description: "Table number if shown" },
    items: {
      type: "array",
      description: "One entry per printed line item. Excludes subtotal, VAT, service, total, rounding and change lines.",
      items: {
        type: "object",
        additionalProperties: false,
        required: ["qty", "description", "unitPrice", "linePrice"],
        properties: {
          qty: { type: "integer", description: "Quantity printed on the line; 1 if not shown" },
          description: { type: "string", description: "Short name, close to what is printed" },
          unitPrice: { type: ["string", "null"], description: "Plain decimal, no currency symbol" },
          linePrice: { type: ["string", "null"], description: "Plain decimal, no currency symbol" }
        }
      }
    },
    subtotal: { type: ["string", "null"] },
    service: {
      type: ["string", "null"],
      description: "Service charge or gratuity already added to the bill"
    },
    vat: {
      type: ["string", "null"],
      description: "VAT shown on the slip. Reported for reference, never added on top."
    },
    total: { type: ["string", "null"] },
    confidence: { type: "string", enum: ["high", "medium", "low"] },
    notes: { type: ["string", "null"], description: "Anything unreadable or unusual about this slip" }
  }
};
var scanBillSchema = z.object({
  /** Base64 without the data: prefix. Capped so one diner cannot exhaust a spend. */
  imageBase64: z.string().min(100).max(12e6),
  mediaType: z.enum(["image/jpeg", "image/png", "image/webp"])
});

// packages/shared/dist/modes.js
var ModeError = class extends Error {
};
function allocateByMode(input) {
  const { mode, billTotalCents, headcount, participantIds } = input;
  if (!Number.isInteger(billTotalCents) || billTotalCents < 0) {
    throw new MoneyError(`billTotalCents must be a non-negative integer, got ${billTotalCents}`);
  }
  if (!Number.isInteger(headcount) || headcount < 1) {
    throw new ModeError(`headcount must be a positive integer, got ${headcount}`);
  }
  if (headcount < participantIds.length) {
    throw new ModeError(`headcount ${headcount} is below the ${participantIds.length} people who joined.`);
  }
  switch (mode) {
    case "equal":
      return allocateEqual(input);
    case "full":
      return allocateFull(input);
    case "items":
      return allocateItems(input);
    default:
      throw new ModeError(`Unknown pay mode: ${String(mode)}`);
  }
}
function allocateEqual(input) {
  const parts = divideCents(input.billTotalCents, input.headcount, input.tieBreakOffset ?? 0);
  const byParticipant = {};
  input.participantIds.forEach((id, i) => {
    byParticipant[id] = parts[i];
  });
  const unassignedCents = parts.slice(input.participantIds.length).reduce((a, b) => a + b, 0);
  return { byParticipant, unassignedCents };
}
function allocateFull(input) {
  const payer = input.hostId ?? input.participantIds[0];
  if (!payer)
    throw new ModeError("Full-bill mode needs somebody to pay it.");
  if (!input.participantIds.includes(payer)) {
    throw new ModeError("The nominated payer is not at this table.");
  }
  const byParticipant = {};
  for (const id of input.participantIds)
    byParticipant[id] = 0;
  byParticipant[payer] = input.billTotalCents;
  return { byParticipant, unassignedCents: 0 };
}
function allocateItems(input) {
  const ids = input.participantIds;
  if (ids.length === 0)
    return { byParticipant: {}, unassignedCents: input.billTotalCents };
  const items = input.itemsByParticipant ?? {};
  const tip = Math.max(0, Math.round(input.tipCents ?? 0));
  const service = Math.max(0, Math.round(input.serviceCents ?? 0));
  const claimed = ids.map((id) => Math.max(0, Math.round(items[id] ?? 0)));
  const claimedTotal = claimed.reduce((a, b) => a + b, 0);
  const itemsPortion = input.billTotalCents - tip - service;
  const unclaimed = Math.max(0, itemsPortion - claimedTotal);
  const serviceParts = allocateProportionalLocal(service, claimed, input.tieBreakOffset ?? 0);
  const tipParts = divideCents(tip, ids.length, input.tieBreakOffset ?? 0);
  const byParticipant = {};
  ids.forEach((id, i) => {
    byParticipant[id] = claimed[i] + serviceParts[i] + tipParts[i];
  });
  return { byParticipant, unassignedCents: unclaimed };
}
function allocateProportionalLocal(amount, weights, offset) {
  const n = weights.length;
  if (n === 0)
    return [];
  if (amount === 0)
    return new Array(n).fill(0);
  const totalWeight = weights.reduce((a, b) => a + b, 0);
  if (totalWeight <= 0)
    return divideCents(amount, n, offset);
  const exact = weights.map((w) => amount * w / totalWeight);
  const floors = exact.map(Math.floor);
  let short = amount - floors.reduce((a, b) => a + b, 0);
  const order = exact.map((value, i) => ({ i, frac: value - Math.floor(value) })).sort((a, b) => b.frac - a.frac || a.i - b.i);
  const out = [...floors];
  for (let k = 0; short > 0; k++, short--) {
    out[order[k % n].i] = out[order[k % n].i] + 1;
  }
  return out;
}

// apps/server/src/middleware/errors.ts
import { ZodError } from "zod";
var ApiError = class _ApiError extends Error {
  constructor(status, message, details) {
    super(message);
    this.status = status;
    this.details = details;
  }
  static badRequest(message, details) {
    return new _ApiError(400, message, details);
  }
  static unauthorized(message = "Join the table before changing anything.") {
    return new _ApiError(401, message);
  }
  static forbidden(message = "You are not allowed to do that.") {
    return new _ApiError(403, message);
  }
  static notFound(message = "Not found.") {
    return new _ApiError(404, message);
  }
  static conflict(message) {
    return new _ApiError(409, message);
  }
  static tooMany(message = "Too many attempts. Wait a minute and try again.") {
    return new _ApiError(429, message);
  }
  static upstream(message) {
    return new _ApiError(502, message);
  }
};
function notFoundHandler(_req, res) {
  res.status(404).json({ ok: false, error: "No such route." });
}
function errorHandler(err, req, res, _next) {
  if (res.headersSent) return;
  if (err instanceof ZodError) {
    const first = err.issues[0];
    res.status(400).json({
      ok: false,
      error: first?.message ?? "That request was not valid.",
      field: first?.path.join(".") || void 0
    });
    return;
  }
  if (err instanceof ApiError) {
    if (err.status >= 500) console.error(`[${req.method} ${req.path}]`, err.message);
    res.status(err.status).json({ ok: false, error: err.message, details: err.details });
    return;
  }
  console.error(`[${req.method} ${req.path}]`, err);
  res.status(500).json({ ok: false, error: "Something went wrong on our side." });
}

// apps/server/src/services/device.ts
import { createHmac, randomBytes as randomBytes2, timingSafeEqual, createHash } from "node:crypto";
var DEVICE_COOKIE = "sharyt_device";
var MAX_AGE_MS = 12 * 60 * 60 * 1e3;
function sign(id, key) {
  return createHmac("sha256", key).update(id).digest("base64url");
}
function verify(value, key) {
  const dot = value.lastIndexOf(".");
  if (dot <= 0) return null;
  const id = value.slice(0, dot);
  const provided = value.slice(dot + 1);
  const expected = sign(id, key);
  const a = Buffer.from(provided);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !timingSafeEqual(a, b)) return null;
  return id;
}
function deviceHash(id) {
  return createHash("sha256").update(id).digest("hex");
}
function readDevice(req, key) {
  const raw = req.cookies?.[DEVICE_COOKIE];
  const existing = raw ? verify(raw, key) : null;
  if (existing) return { id: existing, hash: deviceHash(existing), fresh: false };
  const id = randomBytes2(16).toString("base64url");
  return { id, hash: deviceHash(id), fresh: true };
}
function setDeviceCookie(res, device, key, secure) {
  if (!device.fresh) return;
  res.cookie(DEVICE_COOKIE, `${device.id}.${sign(device.id, key)}`, {
    httpOnly: true,
    sameSite: "lax",
    secure,
    maxAge: MAX_AGE_MS,
    path: "/"
  });
}

// apps/server/src/services/events.ts
function channelFor(restaurantId, sessionId) {
  return `${restaurantId}:${sessionId}`;
}
var EventBus = class {
  subscribers = /* @__PURE__ */ new Map();
  nextId = 1;
  heartbeat = null;
  /** Registers a response as a live stream and returns the unsubscribe hook. */
  subscribe(key, res) {
    const subscriber = { id: this.nextId++, res };
    let set = this.subscribers.get(key);
    if (!set) {
      set = /* @__PURE__ */ new Set();
      this.subscribers.set(key, set);
    }
    set.add(subscriber);
    this.ensureHeartbeat();
    return () => {
      const current = this.subscribers.get(key);
      if (!current) return;
      current.delete(subscriber);
      if (current.size === 0) this.subscribers.delete(key);
      this.maybeStopHeartbeat();
    };
  }
  broadcast(key, event, data = {}) {
    const set = this.subscribers.get(key);
    if (!set || set.size === 0) return;
    const payload = `event: ${event}
data: ${JSON.stringify(data)}

`;
    for (const sub of [...set]) {
      try {
        sub.res.write(payload);
      } catch {
        set.delete(sub);
      }
    }
  }
  subscriberCount() {
    let n = 0;
    for (const set of this.subscribers.values()) n += set.size;
    return n;
  }
  /**
   * A comment line every 25s. Proxies and mobile networks close an idle
   * connection well before a quiet dinner table produces its next event.
   */
  ensureHeartbeat() {
    if (this.heartbeat) return;
    this.heartbeat = setInterval(() => {
      for (const set of this.subscribers.values()) {
        for (const sub of [...set]) {
          try {
            sub.res.write(": keep-alive\n\n");
          } catch {
            set.delete(sub);
          }
        }
      }
    }, 25e3);
    this.heartbeat.unref();
  }
  maybeStopHeartbeat() {
    if (this.subscribers.size === 0 && this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }
  }
  closeAll() {
    for (const set of this.subscribers.values()) {
      for (const sub of set) {
        try {
          sub.res.end();
        } catch {
        }
      }
    }
    this.subscribers.clear();
    this.maybeStopHeartbeat();
  }
};

// apps/server/src/payments/types.ts
var ProviderError = class extends Error {
};
var ProviderConfigError = class extends ProviderError {
};
function centsToMajorString(cents) {
  if (!Number.isInteger(cents) || cents < 0) {
    throw new ProviderError(`Amount must be a non-negative integer of cents, got ${cents}`);
  }
  const whole = Math.floor(cents / 100);
  const part = cents % 100;
  return `${whole}.${String(part).padStart(2, "0")}`;
}
function majorStringToCents(value) {
  const text = typeof value === "number" ? value.toFixed(2) : value.trim();
  if (!/^-?\d+(\.\d{1,2})?$/.test(text)) {
    throw new ProviderError(`Cannot read "${value}" as an amount.`);
  }
  const negative = text.startsWith("-");
  const [whole = "0", frac = ""] = text.replace("-", "").split(".");
  const cents = Number(whole) * 100 + Number(frac.padEnd(2, "0"));
  return negative ? -cents : cents;
}

// apps/server/src/payments/providers/mock.ts
var mock = {
  id: "mock",
  displayName: "Simulated payments",
  blurb: "No money moves. For setting up and training staff before you connect a real account.",
  supportedCurrencies: ["ZAR", "NGN", "GHS", "KES", "USD", "EUR", "GBP", "MUR"],
  simulated: true,
  credentialFields: [],
  async checkCredentials() {
    return { ok: true, live: false, reason: "Payments are simulated. No money will move." };
  },
  async initiate(req, config) {
    const url = new URL(config.returnUrl);
    return {
      kind: "redirect",
      url: `${url.origin}${mockCheckoutPath(config)}?reference=${encodeURIComponent(req.reference)}`
    };
  },
  async verifyWebhook() {
    return { kind: "rejected", reason: "The simulated provider does not receive webhooks." };
  },
  async fetchStatus() {
    return { found: false, status: "unknown", amountCents: null, currency: null, providerRef: null, paidAt: null };
  }
};
function mockCheckoutPath(config) {
  const slug = new URL(config.returnUrl).pathname.split("/").filter(Boolean)[0] ?? "";
  return `/${slug}/mock/checkout`;
}

// apps/server/src/payments/providers/paystack.ts
import { createHmac as createHmac2, timingSafeEqual as timingSafeEqual2 } from "node:crypto";
var API = "https://api.paystack.co";
var paystack = {
  id: "paystack",
  displayName: "Paystack",
  blurb: "Cards, EFT and mobile money across South Africa and the rest of the continent.",
  supportedCurrencies: ["ZAR", "NGN", "GHS", "KES", "USD"],
  simulated: false,
  credentialFields: [
    {
      key: "secret_key",
      label: "Secret key",
      help: "From Settings \u2192 API Keys & Webhooks. Starts sk_live_ for real payments.",
      secret: true
    }
  ],
  async checkCredentials(config) {
    const key = secretKey(config);
    if (!key) return { ok: false, reason: "No secret key." };
    if (!key.startsWith("sk_live_") && !key.startsWith("sk_test_")) {
      return { ok: false, reason: "That does not look like a Paystack secret key." };
    }
    const live = key.startsWith("sk_live_");
    try {
      await call(config, "/balance");
      return { ok: true, live };
    } catch (err) {
      return { ok: false, live, reason: err.message };
    }
  },
  async initiate(req, config) {
    const data = await call(config, "/transaction/initialize", {
      method: "POST",
      body: {
        email: req.email,
        // Straight through: Paystack's minor unit is cents.
        amount: req.amountCents,
        currency: config.currency,
        reference: req.reference,
        callback_url: config.returnUrl,
        metadata: { ...req.metadata, description: req.description }
      }
    });
    return { kind: "redirect", url: data.authorization_url };
  },
  async verifyWebhook(req, config) {
    const key = secretKey(config);
    if (!key) return { kind: "rejected", reason: "No secret key configured." };
    const signature = req.headers["x-paystack-signature"];
    if (!signature) return { kind: "rejected", reason: "No x-paystack-signature header." };
    const expected = createHmac2("sha512", key).update(req.raw).digest("hex");
    if (!safeEqual(expected, signature)) {
      return { kind: "rejected", reason: "Signature did not match." };
    }
    let body;
    try {
      body = JSON.parse(req.raw.toString("utf8"));
    } catch {
      return { kind: "rejected", reason: "Body was not JSON." };
    }
    if (body.event !== "charge.success") {
      return { kind: "ignored", reason: `Event ${body.event ?? "unknown"} is not a payment.` };
    }
    const reference = body.data?.reference;
    if (!reference) return { kind: "rejected", reason: "No reference on the event." };
    const amount = body.data?.amount;
    if (typeof amount !== "number" || !Number.isFinite(amount)) {
      return { kind: "rejected", reason: `Amount was ${JSON.stringify(amount)}.` };
    }
    return {
      kind: "verified",
      event: {
        providerEventId: body.id != null ? String(body.id) : `charge.success:${reference}`,
        reference,
        providerRef: body.data?.id != null ? String(body.data.id) : null,
        status: body.data?.status === "success" ? "succeeded" : "pending",
        amountCents: amount,
        currency: body.data?.currency ?? config.currency,
        paidAt: body.data?.paid_at ?? null
      }
    };
  },
  async fetchStatus(reference, config) {
    try {
      const d = await call(config, `/transaction/verify/${encodeURIComponent(reference)}`);
      return {
        found: true,
        status: d.status === "success" ? "succeeded" : d.status === "failed" ? "failed" : "pending",
        amountCents: d.amount,
        currency: d.currency,
        providerRef: d.id != null ? String(d.id) : null,
        paidAt: d.paid_at ?? null
      };
    } catch {
      return { found: false, status: "unknown", amountCents: null, currency: null, providerRef: null, paidAt: null };
    }
  }
};
function secretKey(config) {
  return (config.credentials.secret_key ?? "").trim();
}
async function call(config, path5, init) {
  const key = secretKey(config);
  if (!key) throw new ProviderConfigError("No Paystack secret key configured.");
  let res;
  try {
    res = await fetch(`${API}${path5}`, {
      method: init?.method ?? "GET",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: init?.body === void 0 ? void 0 : JSON.stringify(init.body),
      signal: AbortSignal.timeout(15e3)
    });
  } catch (err) {
    throw new ProviderError(`Could not reach Paystack: ${err.message}`);
  }
  const text = await res.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new ProviderError(`Paystack returned a non-JSON response (${res.status}).`);
  }
  if (!res.ok || parsed.status === false) {
    throw new ProviderError(parsed.message ?? `Paystack refused the request (${res.status}).`);
  }
  return parsed.data;
}
function safeEqual(a, b) {
  const x = Buffer.from(a, "utf8");
  const y = Buffer.from(b, "utf8");
  if (x.length !== y.length) return false;
  return timingSafeEqual2(x, y);
}

// apps/server/src/payments/providers/payfast.ts
import { createHash as createHash2 } from "node:crypto";
import { promises as dns } from "node:dns";
var LIVE_HOST = "www.payfast.co.za";
var SANDBOX_HOST = "sandbox.payfast.co.za";
var VALID_HOSTS = [
  "www.payfast.co.za",
  "sandbox.payfast.co.za",
  "w1w.payfast.co.za",
  "w2w.payfast.co.za"
];
var payfast = {
  id: "payfast",
  displayName: "Payfast",
  blurb: "Long-established South African gateway. Cards, instant EFT, SnapScan and more. ZAR only.",
  supportedCurrencies: ["ZAR"],
  simulated: false,
  credentialFields: [
    { key: "merchant_id", label: "Merchant ID", secret: false },
    { key: "merchant_key", label: "Merchant key", secret: true },
    {
      key: "passphrase",
      label: "Salt passphrase",
      help: "Set one in Payfast under Settings. Without it the signature can be forged by anyone who knows your merchant ID.",
      secret: true
    }
  ],
  async checkCredentials(config) {
    const { merchant_id, merchant_key, passphrase } = config.credentials;
    if (!merchant_id?.trim() || !merchant_key?.trim()) {
      return { ok: false, reason: "Merchant ID and merchant key are both required." };
    }
    if (!/^\d+$/.test(merchant_id.trim())) {
      return { ok: false, reason: "The merchant ID should be numeric." };
    }
    if (!passphrase?.trim()) {
      return {
        ok: false,
        reason: "Set a salt passphrase in your Payfast account and paste it here. Without one, the ITN signature proves nothing."
      };
    }
    return {
      ok: true,
      live: config.live,
      reason: "Payfast cannot verify credentials without a transaction. Run one test payment before going live."
    };
  },
  async initiate(req, config) {
    if (config.currency !== "ZAR") {
      throw new ProviderConfigError("Payfast settles ZAR only.");
    }
    const { merchant_id, merchant_key } = config.credentials;
    if (!merchant_id || !merchant_key) throw new ProviderConfigError("Payfast is not configured.");
    const fields = {
      merchant_id: merchant_id.trim(),
      merchant_key: merchant_key.trim(),
      return_url: config.returnUrl,
      cancel_url: config.cancelUrl,
      notify_url: config.webhookUrl,
      m_payment_id: req.reference,
      amount: centsToMajorString(req.amountCents),
      item_name: req.description.slice(0, 100)
    };
    if (req.email) fields.email_address = req.email;
    fields.signature = sign2(fields, config.credentials.passphrase ?? "");
    return {
      kind: "form_post",
      url: `https://${config.live ? LIVE_HOST : SANDBOX_HOST}/eng/process`,
      fields
    };
  },
  async verifyWebhook(req, config) {
    const posted = parseForm(req.raw.toString("utf8"));
    if (Object.keys(posted).length === 0) {
      return { kind: "rejected", reason: "Empty ITN body." };
    }
    const provided = posted.signature;
    if (!provided) return { kind: "rejected", reason: "No signature field on the ITN." };
    const { signature: _drop, ...rest } = posted;
    const expected = sign2(rest, config.credentials.passphrase ?? "");
    if (!safeEqual(expected, provided)) {
      return { kind: "rejected", reason: "ITN signature did not match." };
    }
    if (!await sourceIsPayfast(req.sourceIp)) {
      return { kind: "rejected", reason: `ITN came from ${req.sourceIp ?? "an unknown address"}.` };
    }
    if (!await confirmWithPayfast(req.raw.toString("utf8"), config)) {
      return { kind: "rejected", reason: "Payfast did not confirm this ITN." };
    }
    const reference = posted.m_payment_id;
    if (!reference) return { kind: "rejected", reason: "No m_payment_id on the ITN." };
    const status = posted.payment_status;
    if (status !== "COMPLETE") {
      return { kind: "ignored", reason: `Payment status is ${status ?? "missing"}.` };
    }
    let amountCents;
    try {
      amountCents = majorStringToCents(posted.amount_gross ?? "");
    } catch {
      return { kind: "rejected", reason: `Could not read amount_gross "${posted.amount_gross}".` };
    }
    return {
      kind: "verified",
      event: {
        // Payfast supplies no delivery id, so the transaction id is the closest
        // stable thing to deduplicate on.
        providerEventId: posted.pf_payment_id ? `pf:${posted.pf_payment_id}` : null,
        reference,
        providerRef: posted.pf_payment_id ?? null,
        status: "succeeded",
        amountCents,
        currency: "ZAR",
        paidAt: null
      }
    };
  },
  async fetchStatus() {
    return { found: false, status: "unknown", amountCents: null, currency: null, providerRef: null, paidAt: null };
  }
};
function sign2(fields, passphrase) {
  const pairs = Object.entries(fields).filter(([, v]) => v !== void 0 && v !== null && String(v).trim() !== "").map(([k, v]) => `${k}=${phpUrlEncode(String(v).trim())}`);
  if (passphrase.trim()) pairs.push(`passphrase=${phpUrlEncode(passphrase.trim())}`);
  return createHash2("md5").update(pairs.join("&")).digest("hex");
}
function phpUrlEncode(value) {
  return encodeURIComponent(value).replace(/%20/g, "+").replace(/[!'()*~]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}
function parseForm(body) {
  const out = {};
  for (const part of body.split("&")) {
    if (!part) continue;
    const eq = part.indexOf("=");
    const k = eq === -1 ? part : part.slice(0, eq);
    const v = eq === -1 ? "" : part.slice(eq + 1);
    out[decodeURIComponent(k)] = decodeURIComponent(v.replace(/\+/g, " "));
  }
  return out;
}
async function sourceIsPayfast(ip) {
  if (!ip) return false;
  const cleaned = ip.replace(/^::ffff:/, "");
  const resolved = await Promise.all(
    VALID_HOSTS.map(async (host) => {
      try {
        const { address } = await dns.lookup(host, { all: false });
        return address;
      } catch {
        return null;
      }
    })
  );
  return resolved.filter(Boolean).includes(cleaned);
}
async function confirmWithPayfast(rawBody, config) {
  const host = config.live ? LIVE_HOST : SANDBOX_HOST;
  try {
    const res = await fetch(`https://${host}/eng/query/validate`, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: rawBody,
      signal: AbortSignal.timeout(1e4)
    });
    return (await res.text()).trim().toUpperCase().startsWith("VALID");
  } catch {
    return false;
  }
}

// apps/server/src/payments/providers/peach.ts
import { createDecipheriv } from "node:crypto";
var CHECKOUT_LIVE = "https://secure.peachpayments.com";
var CHECKOUT_TEST = "https://testsecure.peachpayments.com";
var SUCCESS = /^(000\.000\.|000\.100\.1|000\.[36])/;
var PENDING = /^(000\.200|800\.400\.5|100\.400\.500)/;
var peach = {
  id: "peach",
  displayName: "Peach Payments",
  blurb: "Cards, EFT, Capitec Pay, Apple Pay and more. Strong South African card coverage.",
  supportedCurrencies: ["ZAR", "USD", "EUR", "GBP", "KES", "MUR"],
  simulated: false,
  credentialFields: [
    { key: "entity_id", label: "Entity ID", help: "The channel you want payments to land in.", secret: false },
    { key: "access_token", label: "Access token", secret: true },
    {
      key: "webhook_key",
      label: "Webhook decryption key",
      help: "The hex key from the webhook section of your Peach dashboard. Without it, webhooks cannot be read at all.",
      secret: true
    }
  ],
  async checkCredentials(config) {
    const { entity_id, access_token, webhook_key } = config.credentials;
    if (!entity_id?.trim()) return { ok: false, reason: "Entity ID is required." };
    if (!access_token?.trim()) return { ok: false, reason: "Access token is required." };
    if (!webhook_key?.trim()) {
      return { ok: false, reason: "The webhook decryption key is required, or payments can never be confirmed." };
    }
    if (!/^[0-9a-fA-F]{64}$/.test(webhook_key.trim())) {
      return { ok: false, reason: "The webhook key should be 64 hex characters (a 256-bit key)." };
    }
    return { ok: true, live: config.live };
  },
  async initiate(req, config) {
    const { entity_id, access_token } = config.credentials;
    if (!entity_id || !access_token) throw new ProviderConfigError("Peach is not configured.");
    const base = config.live ? CHECKOUT_LIVE : CHECKOUT_TEST;
    const body = new URLSearchParams({
      authentication: "",
      entityId: entity_id.trim(),
      // Peach takes a decimal string, not minor units.
      amount: centsToMajorString(req.amountCents),
      currency: config.currency,
      paymentType: "DB",
      merchantTransactionId: req.reference,
      shopperResultUrl: config.returnUrl,
      defaultPaymentMethod: "CARD",
      "customer.email": req.email
    });
    body.delete("authentication");
    let res;
    try {
      res = await fetch(`${base}/v2/checkout`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${access_token.trim()}`,
          "content-type": "application/x-www-form-urlencoded"
        },
        body,
        signal: AbortSignal.timeout(15e3)
      });
    } catch (err) {
      throw new ProviderError(`Could not reach Peach: ${err.message}`);
    }
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.redirectUrl) {
      throw new ProviderError(data.result?.description ?? `Peach refused the request (${res.status}).`);
    }
    return { kind: "redirect", url: data.redirectUrl };
  },
  async verifyWebhook(req, config) {
    const keyHex = (config.credentials.webhook_key ?? "").trim();
    if (!keyHex) return { kind: "rejected", reason: "No webhook decryption key configured." };
    const ivHex = header(req, "x-initialization-vector");
    const tagHex = header(req, "x-authentication-tag");
    if (!ivHex || !tagHex) {
      return { kind: "rejected", reason: "Missing the initialisation vector or authentication tag." };
    }
    let json;
    try {
      const decipher = createDecipheriv(
        "aes-256-gcm",
        Buffer.from(keyHex, "hex"),
        Buffer.from(ivHex, "hex")
      );
      decipher.setAuthTag(Buffer.from(tagHex, "hex"));
      json = Buffer.concat([
        decipher.update(Buffer.from(req.raw.toString("utf8"), "hex")),
        decipher.final()
      ]).toString("utf8");
    } catch {
      return { kind: "rejected", reason: "Could not decrypt the webhook. Wrong key, or the payload was altered." };
    }
    let body;
    try {
      body = JSON.parse(json);
    } catch {
      return { kind: "rejected", reason: "Decrypted body was not JSON." };
    }
    const payload = body.payload ?? body;
    const reference = payload.merchantTransactionId;
    if (!reference) return { kind: "ignored", reason: "No merchantTransactionId; not one of ours." };
    const code = payload.result?.code ?? "";
    const status = SUCCESS.test(code) ? "succeeded" : PENDING.test(code) ? "pending" : "failed";
    let amountCents;
    try {
      amountCents = majorStringToCents(payload.amount ?? "0");
    } catch {
      return { kind: "rejected", reason: `Could not read amount "${payload.amount}".` };
    }
    return {
      kind: "verified",
      event: {
        providerEventId: payload.id ?? null,
        reference,
        providerRef: payload.id ?? null,
        status,
        amountCents,
        currency: payload.currency ?? config.currency,
        paidAt: payload.timestamp ?? null
      }
    };
  },
  async fetchStatus() {
    return { found: false, status: "unknown", amountCents: null, currency: null, providerRef: null, paidAt: null };
  }
};
function header(req, name) {
  const direct = req.headers[name];
  if (direct) return direct;
  const found = Object.entries(req.headers).find(([k]) => k.toLowerCase() === name);
  return found?.[1];
}

// apps/server/src/payments/providers/stitch.ts
import { createHmac as createHmac3, timingSafeEqual as timingSafeEqual3 } from "node:crypto";
var TOKEN_URL = "https://secure.stitch.money/connect/token";
var API_URL = "https://api.stitch.money/graphql";
var TIMESTAMP_TOLERANCE_SECONDS = 5 * 60;
var stitch = {
  id: "stitch",
  displayName: "Stitch",
  blurb: "Pay by bank. The diner approves an EFT in their banking app \u2014 cheaper than card on big bills.",
  supportedCurrencies: ["ZAR"],
  simulated: false,
  credentialFields: [
    { key: "client_id", label: "Client ID", secret: false },
    { key: "client_secret", label: "Client secret", secret: true },
    {
      key: "webhook_secret",
      label: "Webhook signing secret",
      help: "Starts whsec_. From the webhook settings in your Stitch dashboard.",
      secret: true
    }
  ],
  async checkCredentials(config) {
    const { client_id, client_secret, webhook_secret } = config.credentials;
    if (!client_id?.trim() || !client_secret?.trim()) {
      return { ok: false, reason: "Client ID and client secret are both required." };
    }
    if (!webhook_secret?.trim().startsWith("whsec_")) {
      return { ok: false, reason: "The webhook signing secret should start with whsec_." };
    }
    try {
      await accessToken(config);
      return { ok: true, live: config.live };
    } catch (err) {
      return { ok: false, reason: err.message };
    }
  },
  async initiate(req, config) {
    const token = await accessToken(config);
    const query = `
      mutation CreatePaymentRequest($input: CreatePaymentRequestInput!) {
        clientPaymentInitiationRequestCreate(input: $input) {
          paymentInitiationRequest { id url }
        }
      }`;
    const data = await graphql(token, query, {
      input: {
        // Stitch takes minor units, like Paystack -- straight through.
        amount: { quantity: req.amountCents, currency: config.currency },
        payerReference: req.reference.slice(0, 12),
        beneficiaryReference: req.description.slice(0, 12),
        externalReference: req.reference,
        merchant: req.metadata.restaurantName ?? "Sharyt"
      }
    });
    const url = data.clientPaymentInitiationRequestCreate?.paymentInitiationRequest?.url;
    if (!url) throw new ProviderError("Stitch did not return a payment URL.");
    return { kind: "redirect", url };
  },
  async verifyWebhook(req, config) {
    const secret = (config.credentials.webhook_secret ?? "").trim();
    if (!secret) return { kind: "rejected", reason: "No webhook signing secret configured." };
    const verdict = verifySvix(req, secret);
    if (verdict) return verdict;
    let body;
    try {
      body = JSON.parse(req.raw.toString("utf8"));
    } catch {
      return { kind: "rejected", reason: "Body was not JSON." };
    }
    const node = body.data?.client?.paymentInitiationRequest ?? body.paymentInitiationRequest;
    const reference = node?.externalReference;
    if (!reference) return { kind: "ignored", reason: "No externalReference; not one of ours." };
    const state = node?.state?.__typename ?? "";
    const status = state === "PaymentInitiationRequestCompleted" ? "succeeded" : state === "PaymentInitiationRequestCancelled" || state === "PaymentInitiationRequestExpired" ? "failed" : "pending";
    const quantity = node?.amount?.quantity;
    if (typeof quantity !== "number" || !Number.isFinite(quantity)) {
      return { kind: "rejected", reason: `Amount was ${JSON.stringify(quantity)}.` };
    }
    return {
      kind: "verified",
      event: {
        providerEventId: req.headers["svix-id"] ?? node?.id ?? null,
        reference,
        providerRef: node?.id ?? null,
        status,
        amountCents: quantity,
        currency: node?.amount?.currency ?? config.currency,
        paidAt: node?.state?.date ?? null
      }
    };
  },
  async fetchStatus() {
    return { found: false, status: "unknown", amountCents: null, currency: null, providerRef: null, paidAt: null };
  }
};
function verifySvix(req, secret, nowSeconds = Math.floor(Date.now() / 1e3)) {
  const id = req.headers["svix-id"];
  const timestamp = req.headers["svix-timestamp"];
  const signature = req.headers["svix-signature"];
  if (!id || !timestamp || !signature) {
    return { kind: "rejected", reason: "Missing svix-id, svix-timestamp or svix-signature." };
  }
  const sent = Number(timestamp);
  if (!Number.isFinite(sent) || Math.abs(nowSeconds - sent) > TIMESTAMP_TOLERANCE_SECONDS) {
    return { kind: "rejected", reason: "Webhook timestamp is outside the tolerance window." };
  }
  const key = Buffer.from(secret.replace(/^whsec_/, ""), "base64");
  const signed = `${id}.${timestamp}.${req.raw.toString("utf8")}`;
  const expected = createHmac3("sha256", key).update(signed).digest("base64");
  const candidates = signature.split(" ").map((part) => part.split(",")[1]).filter((v) => Boolean(v));
  const match = candidates.some((candidate) => {
    const a = Buffer.from(candidate, "base64");
    const b = Buffer.from(expected, "base64");
    return a.length === b.length && timingSafeEqual3(a, b);
  });
  return match ? null : { kind: "rejected", reason: "Signature did not match." };
}
async function accessToken(config) {
  const { client_id, client_secret } = config.credentials;
  if (!client_id || !client_secret) throw new ProviderConfigError("Stitch is not configured.");
  const res = await fetch(TOKEN_URL, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: client_id.trim(),
      client_secret: client_secret.trim(),
      scope: "client_paymentrequest",
      audience: TOKEN_URL
    }),
    signal: AbortSignal.timeout(15e3)
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.access_token) {
    throw new ProviderError(data.error_description ?? `Stitch refused the token request (${res.status}).`);
  }
  return data.access_token;
}
async function graphql(token, query, variables) {
  const res = await fetch(API_URL, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(15e3)
  });
  const body = await res.json().catch(() => ({}));
  if (body.errors?.length) throw new ProviderError(body.errors.map((e) => e.message).join("; "));
  if (!res.ok || !body.data) throw new ProviderError(`Stitch refused the request (${res.status}).`);
  return body.data;
}

// apps/server/src/payments/providers/yoco.ts
var API2 = "https://payments.yoco.com/api";
var yoco = {
  id: "yoco",
  displayName: "Yoco",
  blurb: "Popular with independent South African venues. Online payments land beside your card machine.",
  supportedCurrencies: ["ZAR"],
  simulated: false,
  credentialFields: [
    {
      key: "secret_key",
      label: "Secret key",
      help: "From Yoco \u2192 Developers. Starts sk_live_ for real payments.",
      secret: true
    },
    {
      key: "webhook_secret",
      label: "Webhook signing secret",
      help: "Starts whsec_. Shown once when you register the webhook.",
      secret: true
    }
  ],
  async checkCredentials(config) {
    const key = (config.credentials.secret_key ?? "").trim();
    if (!key) return { ok: false, reason: "No secret key." };
    const live = key.startsWith("sk_live_");
    if (!live && !key.startsWith("sk_test_")) {
      return { ok: false, reason: "That does not look like a Yoco secret key." };
    }
    if (!(config.credentials.webhook_secret ?? "").trim().startsWith("whsec_")) {
      return { ok: false, reason: "The webhook signing secret should start with whsec_." };
    }
    return { ok: true, live };
  },
  async initiate(req, config) {
    const key = (config.credentials.secret_key ?? "").trim();
    if (!key) throw new ProviderConfigError("Yoco is not configured.");
    let res;
    try {
      res = await fetch(`${API2}/checkouts`, {
        method: "POST",
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
        body: JSON.stringify({
          // Yoco takes cents, like Paystack. Straight through.
          amount: req.amountCents,
          currency: config.currency,
          successUrl: config.returnUrl,
          cancelUrl: config.cancelUrl,
          failureUrl: config.cancelUrl,
          metadata: { ...req.metadata, reference: req.reference }
        }),
        signal: AbortSignal.timeout(15e3)
      });
    } catch (err) {
      throw new ProviderError(`Could not reach Yoco: ${err.message}`);
    }
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.redirectUrl) {
      throw new ProviderError(data.message ?? `Yoco refused the request (${res.status}).`);
    }
    return { kind: "redirect", url: data.redirectUrl };
  },
  async verifyWebhook(req, config) {
    const secret = (config.credentials.webhook_secret ?? "").trim();
    if (!secret) return { kind: "rejected", reason: "No webhook signing secret configured." };
    const rejection = verifySvix(req, secret);
    if (rejection) return rejection;
    let body;
    try {
      body = JSON.parse(req.raw.toString("utf8"));
    } catch {
      return { kind: "rejected", reason: "Body was not JSON." };
    }
    if (body.type !== "payment.succeeded") {
      return { kind: "ignored", reason: `Event ${body.type ?? "unknown"} is not a settled payment.` };
    }
    const reference = body.payload?.metadata?.reference;
    if (!reference) return { kind: "ignored", reason: "No reference in metadata; not one of ours." };
    const amount = body.payload?.amount;
    if (typeof amount !== "number" || !Number.isFinite(amount)) {
      return { kind: "rejected", reason: `Amount was ${JSON.stringify(amount)}.` };
    }
    return {
      kind: "verified",
      event: {
        providerEventId: req.headers["svix-id"] ?? body.id ?? null,
        reference,
        providerRef: body.payload?.id ?? null,
        status: "succeeded",
        amountCents: amount,
        currency: body.payload?.currency ?? config.currency,
        paidAt: body.createdDate ?? null
      }
    };
  },
  async fetchStatus() {
    return { found: false, status: "unknown", amountCents: null, currency: null, providerRef: null, paidAt: null };
  }
};

// apps/server/src/payments/providers/ozow.ts
import { createHash as createHash3 } from "node:crypto";
var LIVE = "https://pay.ozow.com";
var TEST = "https://stagingpay.ozow.com";
var REQUEST_FIELDS = [
  "SiteCode",
  "CountryCode",
  "CurrencyCode",
  "Amount",
  "TransactionReference",
  "BankReference",
  "Optional1",
  "Optional2",
  "Optional3",
  "Optional4",
  "Optional5",
  "Customer",
  "CancelUrl",
  "ErrorUrl",
  "SuccessUrl",
  "NotifyUrl",
  "IsTest"
];
var RESPONSE_FIELDS = [
  "SiteCode",
  "TransactionId",
  "TransactionReference",
  "Amount",
  "Status",
  "Optional1",
  "Optional2",
  "Optional3",
  "Optional4",
  "Optional5",
  "CurrencyCode",
  "IsTest",
  "StatusMessage"
];
var ozow = {
  id: "ozow",
  displayName: "Ozow",
  blurb: "Instant EFT straight from the diner\u2019s bank account. No card needed.",
  supportedCurrencies: ["ZAR"],
  simulated: false,
  credentialFields: [
    { key: "site_code", label: "Site code", secret: false },
    { key: "private_key", label: "Private key", secret: true },
    { key: "api_key", label: "API key", secret: true, optional: true }
  ],
  async checkCredentials(config) {
    if (!config.credentials.site_code?.trim()) return { ok: false, reason: "Site code is required." };
    if (!config.credentials.private_key?.trim()) return { ok: false, reason: "Private key is required." };
    return {
      ok: true,
      live: config.live,
      reason: "Ozow cannot verify credentials without a transaction. Run one test payment before going live."
    };
  },
  async initiate(req, config) {
    const siteCode = config.credentials.site_code?.trim();
    const privateKey = config.credentials.private_key?.trim();
    if (!siteCode || !privateKey) throw new ProviderConfigError("Ozow is not configured.");
    const fields = {
      SiteCode: siteCode,
      CountryCode: "ZA",
      CurrencyCode: config.currency,
      Amount: centsToMajorString(req.amountCents),
      TransactionReference: req.reference,
      // Shows on the diner's bank statement, so it has to say where they were.
      BankReference: req.description.replace(/[^A-Za-z0-9 ]/g, "").slice(0, 20) || "Sharyt",
      CancelUrl: config.cancelUrl,
      ErrorUrl: config.cancelUrl,
      SuccessUrl: config.returnUrl,
      NotifyUrl: config.webhookUrl,
      IsTest: config.live ? "false" : "true"
    };
    fields.HashCheck = hash(REQUEST_FIELDS, fields, privateKey);
    return { kind: "form_post", url: `${config.live ? LIVE : TEST}/`, fields };
  },
  async verifyWebhook(req, config) {
    const privateKey = config.credentials.private_key?.trim();
    if (!privateKey) return { kind: "rejected", reason: "No private key configured." };
    const posted = parseBody(req.raw.toString("utf8"));
    const provided = posted.Hash ?? posted.HashCheck;
    if (!provided) return { kind: "rejected", reason: "No hash on the notification." };
    const expected = hash(RESPONSE_FIELDS, posted, privateKey);
    if (!safeEqual(expected, provided.toLowerCase())) {
      return { kind: "rejected", reason: "Hash did not match." };
    }
    const reference = posted.TransactionReference;
    if (!reference) return { kind: "rejected", reason: "No TransactionReference." };
    const status = (posted.Status ?? "").toLowerCase();
    if (status !== "complete") {
      return { kind: "ignored", reason: `Status is ${posted.Status ?? "missing"}.` };
    }
    let amountCents;
    try {
      amountCents = majorStringToCents(posted.Amount ?? "");
    } catch {
      return { kind: "rejected", reason: `Could not read amount "${posted.Amount}".` };
    }
    return {
      kind: "verified",
      event: {
        providerEventId: posted.TransactionId ? `ozow:${posted.TransactionId}` : null,
        reference,
        providerRef: posted.TransactionId ?? null,
        status: "succeeded",
        amountCents,
        currency: posted.CurrencyCode ?? config.currency,
        paidAt: null
      }
    };
  },
  async fetchStatus() {
    return { found: false, status: "unknown", amountCents: null, currency: null, providerRef: null, paidAt: null };
  }
};
function hash(order, fields, privateKey) {
  const joined = order.map((k) => fields[k] ?? "").join("");
  return createHash3("sha512").update((joined + privateKey).toLowerCase()).digest("hex");
}
function parseBody(body) {
  const trimmed = body.trim();
  if (trimmed.startsWith("{")) {
    try {
      return JSON.parse(trimmed);
    } catch {
      return {};
    }
  }
  const out = {};
  for (const part of trimmed.split("&")) {
    if (!part) continue;
    const eq = part.indexOf("=");
    const k = eq === -1 ? part : part.slice(0, eq);
    const v = eq === -1 ? "" : part.slice(eq + 1);
    out[decodeURIComponent(k)] = decodeURIComponent(v.replace(/\+/g, " "));
  }
  return out;
}

// apps/server/src/payments/index.ts
var PROVIDERS = [mock, paystack, payfast, yoco, stitch, peach, ozow];
var BY_ID = new Map(PROVIDERS.map((p) => [p.id, p]));
function getProvider(id) {
  const found = BY_ID.get(id);
  if (!found) throw new ProviderConfigError(`Unknown payment provider "${id}".`);
  return found;
}
function isProviderId(id) {
  return BY_ID.has(id);
}
function describeProviders(currency) {
  return PROVIDERS.filter((p) => !currency || p.supportedCurrencies.includes(currency)).map((p) => ({
    id: p.id,
    displayName: p.displayName,
    blurb: p.blurb,
    simulated: p.simulated,
    supportedCurrencies: p.supportedCurrencies,
    credentialFields: p.credentialFields
  }));
}
function assertCurrencySupported(provider, currency) {
  if (!provider.supportedCurrencies.includes(currency)) {
    throw new ProviderConfigError(
      `${provider.displayName} does not settle ${currency}. It supports ${provider.supportedCurrencies.join(", ")}.`
    );
  }
}
function missingCredentials(provider, config) {
  return provider.credentialFields.filter((f) => !f.optional && !(config.credentials[f.key] ?? "").trim()).map((f) => f.label);
}

// apps/server/src/services/psp.ts
var PROVIDER_KEY = "psp_provider";
var LIVE_KEY = "psp_live";
var AI_KEY = "ai.anthropic_api_key";
var AI_MODEL = "ai.model";
var AI_EFFORT = "ai.effort";
var credKey = (providerId, field) => `psp.${providerId}.${field}`;
var PspNotConfigured = class extends Error {
};
var PspCredentialUnreadable = class extends Error {
};
async function resolvePsp(ctx, t, urls) {
  const rows = await t.q(
    "SELECT key, value, is_secret FROM restaurant_settings"
  );
  const settings = new Map(rows.map((r) => [r.key, r]));
  const providerId = settings.get(PROVIDER_KEY)?.value ?? "mock";
  if (!isProviderId(providerId)) {
    throw new ProviderConfigError(`This restaurant is set to an unknown payment provider.`);
  }
  const provider = getProvider(providerId);
  assertCurrencySupported(provider, t.restaurant.currency);
  const credentials = {};
  for (const field of provider.credentialFields) {
    const row = settings.get(credKey(providerId, field.key));
    if (!row) continue;
    if (row.is_secret) {
      try {
        credentials[field.key] = decryptSecret(row.value, ctx.appKey);
      } catch {
        throw new PspCredentialUnreadable(
          `Could not decrypt the ${field.label} for ${provider.displayName}. The app secret may have changed since it was saved -- re-enter it in Settings.`
        );
      }
    } else {
      credentials[field.key] = row.value;
    }
  }
  const config = {
    credentials,
    live: settings.get(LIVE_KEY)?.value === "true",
    currency: t.restaurant.currency,
    ...urls
  };
  const missing = missingCredentials(provider, config);
  if (missing.length > 0) {
    throw new PspNotConfigured(
      `${provider.displayName} is missing: ${missing.join(", ")}. Add them in Settings.`
    );
  }
  return { provider, config };
}
async function pspSummary(ctx, t) {
  const rows = await t.q(
    "SELECT key, value, is_secret FROM restaurant_settings"
  );
  const settings = new Map(rows.map((r) => [r.key, r]));
  const providerId = settings.get(PROVIDER_KEY)?.value ?? "mock";
  const provider = getProvider(isProviderId(providerId) ? providerId : "mock");
  const fields = provider.credentialFields.map((f) => {
    const row = settings.get(credKey(provider.id, f.key));
    return {
      key: f.key,
      label: f.label,
      ...f.help ? { help: f.help } : {},
      secret: f.secret,
      set: Boolean(row?.value),
      // A masked tail, never the value. Enough to tell one key from another
      // when an operator is checking which account they pasted.
      hint: row && !f.secret ? row.value : row ? "\u2022\u2022\u2022\u2022" : null
    };
  });
  return {
    providerId: provider.id,
    displayName: provider.displayName,
    simulated: provider.simulated,
    live: settings.get(LIVE_KEY)?.value === "true",
    liveEnabledAt: t.restaurant.liveEnabledAt,
    fields,
    missing: fields.filter((f) => !f.set && !provider.credentialFields.find((c) => c.key === f.key)?.optional).map((f) => f.label)
  };
}
async function setProvider(t, providerId) {
  if (!isProviderId(providerId)) throw new ProviderConfigError(`Unknown payment provider.`);
  const provider = getProvider(providerId);
  assertCurrencySupported(provider, t.restaurant.currency);
  await upsert(t, PROVIDER_KEY, providerId, false);
  await upsert(t, LIVE_KEY, "false", false);
  await t.q("UPDATE restaurants SET mock_mode = $2 WHERE id = $1", [t.restaurantId, provider.simulated]);
}
async function saveCredentials(ctx, t, providerId, values) {
  const provider = getProvider(providerId);
  for (const field of provider.credentialFields) {
    const raw = values[field.key];
    if (raw === void 0 || raw.trim() === "") continue;
    const stored = field.secret ? encryptSecret(raw.trim(), ctx.appKey) : raw.trim();
    await upsert(t, credKey(provider.id, field.key), stored, field.secret);
  }
  await upsert(t, LIVE_KEY, "false", false);
}
async function clearCredential(t, providerId, fieldKey) {
  await t.q("DELETE FROM restaurant_settings WHERE key = $1", [credKey(providerId, fieldKey)]);
  await upsert(t, LIVE_KEY, "false", false);
}
async function attemptGoLive(ctx, t, urls) {
  const { provider, config } = await resolvePsp(ctx, t, urls);
  if (provider.simulated) {
    return { ok: false, reason: "Simulated payments cannot go live. Choose a real provider first." };
  }
  const check = await provider.checkCredentials({ ...config, live: true });
  if (!check.ok) return { ok: false, reason: check.reason ?? "Those credentials were refused." };
  if (check.live === false) {
    return {
      ok: false,
      reason: check.reason ?? "Those are test credentials. Test payments never settle -- paste the live ones before going live."
    };
  }
  await upsert(t, LIVE_KEY, "true", false);
  await ctx.registry.enableLive(t.restaurantId);
  return { ok: true };
}
async function resolveVision(ctx, t) {
  const rows = await t.q(
    "SELECT key, value, is_secret FROM restaurant_settings WHERE key LIKE $1",
    ["ai.%"]
  );
  const settings = new Map(rows.map((r) => [r.key, r]));
  let apiKey = "";
  const stored = settings.get(AI_KEY);
  if (stored) {
    try {
      apiKey = decryptSecret(stored.value, ctx.appKey);
    } catch {
      throw new PspCredentialUnreadable(
        "Could not decrypt the Anthropic API key. Re-enter it in Settings."
      );
    }
  }
  return {
    apiKey,
    model: settings.get(AI_MODEL)?.value || "claude-opus-5",
    effort: settings.get(AI_EFFORT)?.value || "medium"
  };
}
async function saveVisionSettings(ctx, t, values) {
  if (values.apiKey?.trim()) {
    await upsert(t, AI_KEY, encryptSecret(values.apiKey.trim(), ctx.appKey), true);
  }
  if (values.model?.trim()) await upsert(t, AI_MODEL, values.model.trim(), false);
  if (values.effort?.trim()) await upsert(t, AI_EFFORT, values.effort.trim(), false);
}
async function visionSummary(t) {
  const rows = await t.q(
    "SELECT key, value FROM restaurant_settings WHERE key LIKE $1",
    ["ai.%"]
  );
  const settings = new Map(rows.map((r) => [r.key, r.value]));
  return {
    configured: Boolean(settings.get(AI_KEY)),
    model: settings.get(AI_MODEL) ?? "claude-opus-5",
    effort: settings.get(AI_EFFORT) ?? "medium",
    spentMicros: await t.telemetry.aiSpendMicros()
  };
}
async function upsert(t, key, value, secret) {
  await t.q(
    `INSERT INTO restaurant_settings (restaurant_id, key, value, is_secret, updated_at)
     VALUES ($1, $2, $3, $4, now())
     ON CONFLICT (restaurant_id, key)
       DO UPDATE SET value = EXCLUDED.value, is_secret = EXCLUDED.is_secret, updated_at = now()`,
    [t.restaurantId, key, value, secret]
  );
}

// apps/server/src/services/vision.ts
import Anthropic from "@anthropic-ai/sdk";
var VisionNotConfigured = class extends Error {
  constructor() {
    super("No Anthropic API key is set for this restaurant. Add one in Settings, or enter the bill by hand.");
  }
};
var VisionRefused = class extends Error {
};
var VisionError = class extends Error {
};
var DEFAULT_PRICING = {
  "claude-opus-5": { input: 5, output: 25 },
  "claude-sonnet-5": { input: 3, output: 15 },
  "claude-haiku-4-5": { input: 1, output: 5 }
};
var SYSTEM = `You read restaurant till slips and return structured data.

Rules:
- Amounts are plain decimal strings with a period as the decimal separator and no currency symbol.
- One entry per printed line item. "2 CASTLE LITE 78.00" is qty 2 with linePrice 78.00.
- Keep descriptions close to what is printed. Expand only obvious abbreviations.
- Do NOT include subtotal, VAT, service, total, rounding or change lines in "items".
- If a service charge is printed as a separate line, put it in "service", not in "items".
- South African menu prices are VAT-inclusive. Report VAT if printed, but it is a
  breakdown of the total, never an addition to it.
- If an amount is unreadable, return null for it and say so in "notes". Never guess a price.`;
var VisionService = class {
  constructor(config, telemetry) {
    this.config = config;
    this.telemetry = telemetry;
  }
  isConfigured() {
    return this.config.apiKey.trim() !== "";
  }
  async scanBill(input) {
    if (!this.isConfigured()) throw new VisionNotConfigured();
    const client = new Anthropic({ apiKey: this.config.apiKey.trim() });
    const started = Date.now();
    let message;
    try {
      message = await client.messages.create({
        model: this.config.model,
        // Generous on purpose. Thinking is on by default and `max_tokens` caps
        // thinking plus response together, so a tight cap truncates mid-answer.
        max_tokens: 16e3,
        system: SYSTEM,
        output_config: {
          effort: this.config.effort,
          format: { type: "json_schema", schema: BILL_SCAN_JSON_SCHEMA }
        },
        messages: [
          {
            role: "user",
            content: [
              {
                type: "image",
                source: { type: "base64", media_type: input.mediaType, data: input.imageBase64 }
              },
              { type: "text", text: "Read this till slip." }
            ]
          }
        ]
      });
    } catch (err) {
      await this.record(input.sessionId, started, null, false, err.message);
      if (err instanceof Anthropic.AuthenticationError) {
        throw new VisionError("That Anthropic API key was refused. Check it in Settings.");
      }
      if (err instanceof Anthropic.RateLimitError) {
        throw new VisionError("Anthropic is rate-limiting us. Try again in a moment.");
      }
      throw new VisionError(`Could not read the slip: ${err.message}`);
    }
    await this.record(
      input.sessionId,
      started,
      message,
      message.stop_reason !== "refusal",
      message.stop_reason === "refusal" ? "refusal" : null
    );
    if (message.stop_reason === "refusal") {
      throw new VisionRefused("The model would not read that image. Enter the bill by hand.");
    }
    return interpret(textOf(message));
  }
  async record(sessionId, started, message, ok, error) {
    const usage = message?.usage;
    const model = message?.model ?? this.config.model;
    const price = this.config.pricing[model] ?? DEFAULT_PRICING[model] ?? { input: 0, output: 0 };
    const costMicros = usage ? Math.round(
      usage.input_tokens / 1e6 * price.input * 1e6 + usage.output_tokens / 1e6 * price.output * 1e6
    ) : 0;
    await this.telemetry.recordAiUsage({
      sessionId,
      operation: "scan_bill",
      model,
      inputTokens: usage?.input_tokens ?? 0,
      outputTokens: usage?.output_tokens ?? 0,
      cacheReadTokens: usage?.cache_read_input_tokens ?? 0,
      cacheWriteTokens: usage?.cache_creation_input_tokens ?? 0,
      costMicros,
      latencyMs: Date.now() - started,
      ok,
      error
    });
  }
};
function textOf(message) {
  return message.content.filter((b) => b.type === "text").map((b) => b.text).join("").trim();
}
function interpret(raw) {
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new VisionError("The slip could not be read. Enter the bill by hand.");
  }
  const items = (parsed.items ?? []).map((line) => {
    const qty = Math.max(1, Math.trunc(Number(line.qty) || 1));
    const unitCents = parseAmountToCents(line.unitPrice);
    let lineCents = parseAmountToCents(line.linePrice);
    if (!lineCents && unitCents) lineCents = unitCents * qty;
    return {
      qty,
      description: String(line.description ?? "ITEM").trim().slice(0, 80) || "ITEM",
      unitCents: unitCents || (qty ? Math.round(lineCents / qty) : lineCents),
      lineCents
    };
  }).filter((line) => line.lineCents > 0);
  const itemsTotalCents = items.reduce((sum, line) => sum + line.lineCents, 0);
  const serviceCents = parseAmountToCents(parsed.service);
  const statedTotalCents = parseAmountToCents(parsed.total);
  const computed = itemsTotalCents + serviceCents;
  const drift = statedTotalCents > 0 ? Math.abs(computed - statedTotalCents) : 0;
  return {
    venue: parsed.venue?.trim() || null,
    table: parsed.table?.trim() || null,
    items,
    serviceCents,
    vatCents: parseAmountToCents(parsed.vat),
    statedTotalCents,
    itemsTotalCents,
    confidence: parsed.confidence ?? "medium",
    notes: parsed.notes?.trim() || null,
    // A rand of slack absorbs the rounding line some tills print. More than
    // that is a real disagreement and the host has to look.
    mismatch: drift > 100 ? `The lines add up to ${(computed / 100).toFixed(2)} but the slip says ${(statedTotalCents / 100).toFixed(2)}. Check for a missed line.` : null
  };
}

// apps/server/src/middleware/rateLimit.ts
var RateLimiter = class {
  buckets = /* @__PURE__ */ new Map();
  sweeper;
  constructor() {
    this.sweeper = setInterval(() => this.sweep(), 12e4);
    this.sweeper.unref();
  }
  /**
   * `key` should identify the actor and the action together, e.g.
   * `code-miss:203.0.113.4` or `login:someone@example.com`.
   */
  hit(key, limit, windowMs, now = Date.now()) {
    const bucket = this.buckets.get(key);
    if (!bucket || now > bucket.resetAt) {
      this.buckets.set(key, { count: 1, resetAt: now + windowMs });
      return { allowed: true, retryAfter: 0, remaining: limit - 1 };
    }
    bucket.count += 1;
    if (bucket.count > limit) {
      return { allowed: false, retryAfter: Math.ceil((bucket.resetAt - now) / 1e3), remaining: 0 };
    }
    return { allowed: true, retryAfter: 0, remaining: limit - bucket.count };
  }
  /** Check a window without consuming from it. */
  peek(key, limit, now = Date.now()) {
    const bucket = this.buckets.get(key);
    if (!bucket || now > bucket.resetAt) return true;
    return bucket.count <= limit;
  }
  reset(key) {
    this.buckets.delete(key);
  }
  sweep(now = Date.now()) {
    for (const [key, bucket] of this.buckets) {
      if (now > bucket.resetAt) this.buckets.delete(key);
    }
  }
  stop() {
    clearInterval(this.sweeper);
  }
};
var tenantKey = (restaurantId, action, subject) => `t:${restaurantId}:${action}:${subject}`;
var platformKey = (action, subject) => `p:${action}:${subject}`;
var LIMITS = {
  /**
   * Only misses are counted, so a table in active use is never throttled while
   * the ~24M code space stays unwalkable.
   */
  /**
   * Misses only, so a table in active use is never throttled. Table codes are
   * short and admin-chosen, so the code space is not the defence -- the roster
   * lock and host ejection are.
   */
  codeMiss: { limit: 30, windowMs: 6e4 },
  /** Per table as well as per IP: rotating X-Forwarded-For is free. */
  joinPerTable: { limit: 40, windowMs: 3e5 },
  join: { limit: 30, windowMs: 6e4 },
  /** Vision calls cost real money; the host is the only one who can make them. */
  scan: { limit: 12, windowMs: 3e5 },
  claimText: { limit: 30, windowMs: 3e5 },
  remind: { limit: 10, windowMs: 3e5 },
  /** Any member can trigger this and it fans out one Paystack call per unpaid person. */
  verify: { limit: 6, windowMs: 6e4 },
  loginIp: { limit: 20, windowMs: 9e5 },
  loginAccount: { limit: 8, windowMs: 9e5 },
  setup: { limit: 10, windowMs: 9e5 }
};

// apps/server/src/services/settings.ts
var SETTINGS = [
  /* ------------------------------------------------------------- tips -- */
  {
    key: "tip.presets",
    type: "json",
    default: "[0, 10, 12.5, 15]",
    label: "Tip options",
    help: "The percentages a diner is offered. First one is preselected.",
    group: "tips"
  },
  {
    key: "tip.default",
    type: "number",
    default: "10",
    label: "Default tip",
    help: "Preselected when the host reaches the tip step.",
    group: "tips"
  },
  {
    key: "tip.allocation",
    type: "string",
    default: "equal",
    label: "How the tip is divided",
    help: "Equal splits it per head. Proportional charges it in the same ratio as what each person ate. Only affects itemised bills; the other modes have one number anyway.",
    group: "tips"
  },
  {
    key: "service.charge_percent",
    type: "number",
    default: "0",
    label: "Service charge",
    help: "Added automatically. Leave at 0 if you do not add one.",
    group: "tips"
  },
  /* ------------------------------------------------------------ brand -- */
  {
    key: "brand.display_name",
    type: "string",
    default: "",
    label: "Name shown to diners",
    help: "Defaults to your restaurant name.",
    group: "brand"
  },
  {
    key: "brand.accent",
    type: "string",
    default: "#a73a00",
    label: "Accent colour",
    help: "Used for buttons and the amount a diner owes.",
    group: "brand"
  },
  {
    key: "brand.closing_note",
    type: "string",
    default: "",
    label: "Thank-you note",
    help: "Shown once a diner has paid. Leave blank for nothing.",
    group: "brand"
  },
  /* ---------------------------------------------------------- service -- */
  {
    key: "service.idle_minutes",
    type: "number",
    default: "120",
    label: "Close abandoned tables after",
    help: "Minutes of no activity before a table is given up on. Never fires while somebody has a payment in progress.",
    group: "service"
  },
  {
    key: "service.receipt_hours",
    type: "number",
    default: "2",
    label: "Diners can see their receipt for",
    help: "Hours after a table closes. Kept short because the next party scans the same QR code, and only people who were at the table can see it at all.",
    group: "service"
  },
  {
    key: "service.require_cashier_confirmation",
    type: "boolean",
    default: "false",
    label: "Cashier confirms the total before anyone pays",
    help: "Slower, but a misread bill cannot be paid before somebody checks it.",
    group: "service"
  }
];
var BY_KEY = new Map(SETTINGS.map((s) => [s.key, s]));
async function readSettings(t) {
  const rows = await t.q(
    "SELECT key, value FROM restaurant_settings"
  );
  const stored = new Map(rows.map((r) => [r.key, r.value]));
  const raw = (key) => stored.get(key) ?? BY_KEY.get(key)?.default ?? "";
  const presets = safeJson(raw("tip.presets"), [0, 10, 12.5, 15]).filter(
    (n) => typeof n === "number" && n >= 0 && n <= 100
  );
  return {
    // A restaurant that clears every preset would otherwise offer a diner no
    // way to tip at all, which is not what they meant.
    tipPresets: presets.length > 0 ? presets : [0, 10, 12.5, 15],
    tipDefault: Number(raw("tip.default")) || 0,
    tipAllocation: raw("tip.allocation") === "proportional" ? "proportional" : "equal",
    serviceChargePercent: Number(raw("service.charge_percent")) || 0,
    displayName: raw("brand.display_name") || t.restaurant.name,
    accent: /^#[0-9a-fA-F]{6}$/.test(raw("brand.accent")) ? raw("brand.accent") : "#a73a00",
    closingNote: raw("brand.closing_note"),
    idleMinutes: clamp(Number(raw("service.idle_minutes")) || 120, 15, 1440),
    receiptHours: clamp(Number(raw("service.receipt_hours")) || 2, 1, 72),
    requireCashierConfirmation: raw("service.require_cashier_confirmation") === "true"
  };
}
async function describeSettings(t) {
  const rows = await t.q(
    "SELECT key, value FROM restaurant_settings"
  );
  const stored = new Map(rows.map((r) => [r.key, r.value]));
  return SETTINGS.map((spec) => ({ ...spec, value: stored.get(spec.key) ?? spec.default }));
}
var UnknownSetting = class extends Error {
};
async function writeSettings(t, values) {
  for (const [key, value] of Object.entries(values)) {
    const spec = BY_KEY.get(key);
    if (!spec) throw new UnknownSetting(`There is no setting called "${key}".`);
    await t.q(
      `INSERT INTO restaurant_settings (restaurant_id, key, value, is_secret, updated_at)
       VALUES ($1, $2, $3, FALSE, now())
       ON CONFLICT (restaurant_id, key)
         DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
      [t.restaurantId, key, String(value)]
    );
  }
}
function safeJson(raw, fallback) {
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : fallback;
  } catch {
    return fallback;
  }
}
var clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, n));

// apps/server/src/services/sessionService.ts
var UnassignedSharesError = class extends Error {
};
var BillNotFrozenError = class extends Error {
};
var WrongModeError = class extends Error {
};
function computeOwed(full) {
  const { session, participants, items, claims } = full;
  if (session.billTotalCents === null || !session.payMode || !session.headcount) {
    return { byParticipant: {}, unassignedCents: 0 };
  }
  const active = participants.filter((p) => p.removedAt === null);
  const ids = active.map((p) => p.id);
  const host = active.find((p) => p.isHost);
  const itemsByParticipant = {};
  if (session.payMode === "items") {
    for (const id of ids) itemsByParticipant[id] = 0;
    for (const item of items) {
      const claimants = claims.filter((c) => c.itemId === item.id).map((c) => c.participantId);
      if (claimants.length === 0) continue;
      const each = Math.floor(item.lineCents / claimants.length);
      let spare = item.lineCents - each * claimants.length;
      for (const pid of claimants) {
        itemsByParticipant[pid] = (itemsByParticipant[pid] ?? 0) + each + (spare-- > 0 ? 1 : 0);
      }
    }
  }
  return allocateByMode({
    mode: session.payMode,
    billTotalCents: session.billTotalCents,
    headcount: session.headcount,
    participantIds: ids,
    hostId: host?.id ?? null,
    itemsByParticipant,
    tipCents: session.tipCents,
    serviceCents: session.serviceCents,
    // Stable per session, so the spare cent does not hop between diners when
    // the roster is re-read -- somebody would otherwise see an amount that is
    // not the one their checkout was raised for.
    tieBreakOffset: offsetFor(session.id)
  });
}
function offsetFor(sessionId) {
  let h = 0;
  for (let i = 0; i < sessionId.length; i++) h = h * 31 + sessionId.charCodeAt(i) >>> 0;
  return h;
}
async function publicView(t, full, viewerId) {
  const table = await t.tables.byId(full.session.tableId);
  const payments = await t.payments.forSession(full.session.id);
  const settings = await readSettings(t);
  const owed = computeOwed(full);
  const recon = summarise(payments);
  const active = full.participants.filter((p) => p.removedAt === null);
  const viewer = active.find((p) => p.id === viewerId) ?? null;
  const participants = active.map((p) => ({
    id: p.id,
    name: p.name,
    isHost: p.isHost,
    seatNo: p.seatNo,
    oweCents: owed.byParticipant[p.id] ?? null,
    paid: recon.paidParticipants.has(p.id)
  }));
  const items = full.items.map((i) => ({
    id: i.id,
    qty: i.qty,
    description: i.description,
    lineCents: i.lineCents,
    claimedBy: full.claims.filter((c) => c.itemId === i.id).map((c) => c.participantId)
  }));
  return {
    id: full.session.id,
    status: full.session.status,
    payMode: full.session.payMode,
    restaurant: {
      slug: t.restaurant.slug,
      // What the restaurant calls itself to diners, which is not always its
      // legal name.
      name: settings.displayName,
      currency: t.restaurant.currency,
      // Said out loud. A diner should never be unsure whether they really paid.
      mockPayments: t.restaurant.mockMode,
      accent: settings.accent,
      closingNote: settings.closingNote,
      tipPresets: settings.tipPresets,
      tipDefault: settings.tipDefault
    },
    table: {
      code: table?.code ?? "?",
      label: table?.label ?? null,
      seats: table?.seats ?? 0
    },
    currency: full.session.currency,
    headcount: full.session.headcount,
    headcountLocked: full.session.headcountLockedAt !== null,
    itemsCents: full.session.itemsCents,
    serviceCents: full.session.serviceCents,
    tipCents: full.session.tipCents,
    vatCents: full.session.vatCents,
    billTotalCents: full.session.billTotalCents,
    billFrozen: full.session.billFrozenAt !== null,
    unassignedCents: owed.unassignedCents,
    paidCents: recon.paidCents,
    participants,
    items,
    you: viewer ? { id: viewer.id, isHost: viewer.isHost } : null
  };
}
function summarise(payments) {
  let paidCents = 0;
  const paidParticipants = /* @__PURE__ */ new Set();
  for (const p of payments) {
    if (p.status === "paid" && p.voidedAt === null) {
      paidCents += p.receivedCents ?? 0;
      paidParticipants.add(p.onBehalfOf ?? p.participantId);
    }
  }
  return { paidCents, paidParticipants };
}
async function raiseCheckout(t, psp, req) {
  const full = await t.sessions.full(req.sessionId);
  if (!full) throw new Error("No such session.");
  const { session } = full;
  if (session.billTotalCents === null || !session.payMode) {
    throw new BillNotFrozenError("The bill has not been finalised yet.");
  }
  const owed = computeOwed(full);
  const target = req.onBehalfOf ?? req.payerId;
  if (session.payMode === "full" && !req.coverUnassigned) {
    const host = full.participants.find((p) => p.isHost && p.removedAt === null);
    if (!host || host.id !== req.payerId) {
      throw new WrongModeError("Your host is paying for this table.");
    }
  }
  let amountCents;
  if (req.coverUnassigned) {
    amountCents = owed.unassignedCents;
  } else if (req.onBehalfOf) {
    const theirs = owed.byParticipant[req.onBehalfOf];
    if (theirs === void 0) throw new Error("That person is not at this table.");
    amountCents = theirs;
  } else {
    amountCents = owed.byParticipant[req.payerId] ?? 0;
  }
  if (amountCents <= 0) {
    throw new UnassignedSharesError(
      req.coverUnassigned ? "There is nothing left to cover." : "There is nothing for you to pay."
    );
  }
  const existing = req.coverUnassigned ? null : await t.payments.liveForParticipant(session.id, target);
  if (existing?.status === "paid") throw new Error("That share is already paid.");
  if (existing && existing.expectedCents === amountCents) {
    return {
      payment: existing,
      amountCents,
      handoff: { kind: "redirect", url: existing.authorizationUrl ?? "" }
    };
  }
  if (existing) await t.payments.voidPending(session.id);
  const reference = buildReference(t.restaurantId, session.id, target);
  const payer = full.participants.find((p) => p.id === req.payerId);
  const email = payer?.email ?? `guest${payer?.seatNo ?? 0}+${session.id}@${t.restaurant.slug}.sharyt.invalid`;
  const handoff = await psp.provider.initiate(
    {
      reference,
      amountCents,
      description: `${t.restaurant.name} table ${req.tableCode}`,
      email,
      metadata: {
        restaurantId: t.restaurantId,
        sessionId: session.id,
        participantId: target,
        restaurantName: t.restaurant.name
      }
    },
    psp.config
  );
  const payment = await t.payments.createPending({
    sessionId: session.id,
    participantId: req.payerId,
    onBehalfOf: req.onBehalfOf ?? null,
    reference,
    expectedCents: amountCents,
    currency: session.currency,
    provider: psp.provider.id,
    // A redirect can be stored and re-offered. A form post cannot -- the fields
    // are signed and short-lived -- so the diner is sent to our own page, which
    // rebuilds and submits it.
    authorizationUrl: handoff.kind === "redirect" ? handoff.url : `/${t.restaurant.slug}/pay/${encodeURIComponent(reference)}`
  });
  return { payment, amountCents, handoff };
}
async function reconcile(t, sessionId) {
  const full = await t.sessions.full(sessionId);
  if (!full) throw new Error("No such session.");
  const owed = computeOwed(full);
  const money = await t.payments.reconcile(sessionId);
  const billTotalCents = full.session.billTotalCents ?? 0;
  const assigned = Object.entries(owed.byParticipant).filter(([, cents]) => cents > 0);
  const everyShareCovered = assigned.every(([pid]) => money.paidParticipants.includes(pid));
  return {
    billTotalCents,
    paidCents: money.paidCents,
    underpaidCents: money.underpaidCents,
    staleCents: money.staleCents,
    unassignedCents: owed.unassignedCents,
    outstandingCents: Math.max(0, billTotalCents - money.paidCents),
    everyShareCovered,
    // Two conditions, and both are needed.
    //
    // The total alone is not enough: one diner paying twice would otherwise
    // reach the target and close the table while somebody else never paid and
    // still holds a live, payable checkout.
    //
    // `unassignedCents === 0` is deliberately *not* a third condition. It is a
    // structural figure -- headcount minus roster, which stays put when two
    // people share a phone -- so requiring it to be zero would mean such a
    // table could never close even after every cent had arrived. The money
    // condition already covers it: the unassigned portion is part of
    // `billTotalCents`, so the total cannot be reached until someone pays it.
    canClosePaid: billTotalCents > 0 && money.paidCents >= billTotalCents && everyShareCovered
  };
}
function buildReference(restaurantId, sessionId, participantId) {
  const rand = Math.random().toString(36).slice(2, 8);
  return `${restaurantId}_${sessionId}_${participantId}_${rand}`.toLowerCase().replace(/[^a-z0-9_.-]/g, "");
}
async function maybeSettle(t, sessionId) {
  const session = await t.sessions.byId(sessionId);
  if (!session || session.status === "paid") return false;
  const r = await reconcile(t, sessionId);
  if (!r.canClosePaid) return false;
  await t.sessions.setStatus(sessionId, "paid");
  return true;
}
async function invalidateCheckouts(t, session) {
  return t.payments.voidPending(session.id);
}

// apps/server/src/db/sessions.ts
var NameTakenError = class extends Error {
};
var TableBusyError = class extends Error {
};
var StaleVersionError = class extends Error {
};
var S_COLS = `id, table_id, status, pay_mode, headcount, headcount_locked_at, currency,
                items_cents, service_cents, tip_cents, vat_cents, bill_total_cents,
                bill_frozen_at, unassigned_cents, cashier_user_id, closed_at, short_cents,
                version, created_at, last_activity_at`;
var P_COLS = `id, session_id, name, email, is_host, device_hash, seat_no, joined_at, removed_at`;
function mapSession(r) {
  return {
    id: r.id,
    tableId: r.table_id,
    status: r.status,
    payMode: r.pay_mode ?? null,
    headcount: r.headcount ?? null,
    headcountLockedAt: iso(r.headcount_locked_at),
    currency: r.currency,
    itemsCents: r.items_cents,
    serviceCents: r.service_cents,
    tipCents: r.tip_cents,
    vatCents: r.vat_cents,
    billTotalCents: r.bill_total_cents ?? null,
    billFrozenAt: iso(r.bill_frozen_at),
    unassignedCents: r.unassigned_cents,
    cashierUserId: r.cashier_user_id ?? null,
    closedAt: iso(r.closed_at),
    shortCents: r.short_cents ?? null,
    version: r.version,
    createdAt: isoRequired(r.created_at),
    lastActivityAt: isoRequired(r.last_activity_at)
  };
}
function mapParticipant(r) {
  return {
    id: r.id,
    sessionId: r.session_id,
    name: r.name ?? null,
    email: r.email ?? null,
    isHost: r.is_host,
    deviceHash: r.device_hash ?? null,
    seatNo: r.seat_no ?? null,
    joinedAt: isoRequired(r.joined_at),
    removedAt: iso(r.removed_at)
  };
}
var LIVE_STATUSES = ["open", "locked", "awaiting_payment"];
var TOKEN_TTL_HOURS = 4;
var RECEIPT_WINDOW_HOURS = 2;
var SessionRepository = class {
  constructor(q, restaurantId) {
    this.q = q;
    this.restaurantId = restaurantId;
  }
  /* ------------------------------------------------------------ lifecycle */
  /**
   * Staff seat a table. The partial unique index on live statuses is what makes
   * "one open seating per table" true under concurrency rather than only when
   * nobody double-taps.
   */
  async open(tableId, opts) {
    try {
      const row = await this.q.one(
        `INSERT INTO sessions (id, restaurant_id, table_id, currency, opened_by_user_id, cashier_user_id)
         VALUES ($1, $2, $3, $4, $5, $5)
         RETURNING ${S_COLS}`,
        [newId("ses"), this.restaurantId, tableId, opts.currency, opts.openedBy ?? null]
      );
      return mapSession(row);
    } catch (err) {
      if (String(err.message).includes("idx_sessions_one_open")) {
        throw new TableBusyError("That table already has a session open.");
      }
      throw err;
    }
  }
  /** The session a diner scanning this table should land in, if any. */
  async liveForTable(tableId) {
    const row = await this.q.one(
      `SELECT ${S_COLS} FROM sessions
        WHERE table_id = $1 AND status = ANY($2)
        ORDER BY created_at DESC LIMIT 1`,
      [tableId, LIVE_STATUSES]
    );
    return row ? mapSession(row) : null;
  }
  /**
   * The most recent session at this table, live or not.
   *
   * Reads use this; joins and mutations use `liveForTable`. A diner who has
   * just paid still needs their close screen and their receipt, and the table
   * stops being live the instant it settles -- so a reader restricted to live
   * sessions shows the person who just paid a 404.
   */
  async latestForTable(tableId) {
    const row = await this.q.one(
      `SELECT ${S_COLS} FROM sessions
        WHERE table_id = $1
        ORDER BY created_at DESC LIMIT 1`,
      [tableId]
    );
    return row ? mapSession(row) : null;
  }
  /**
   * What a given device is allowed to see at this table.
   *
   * A live session is public to whoever is standing there -- they are about to
   * join it. A finished one is visible only to someone who was actually in it,
   * and only briefly after it closed.
   *
   * This is the whole answer to "the next customer scans the very same QR
   * code". The sticker is permanent, so without this check tomorrow's diners
   * would be handed last night's bill: the names at the table, the itemised
   * total and who paid what.
   */
  async readableForTable(tableId, deviceHash2) {
    const live = await this.liveForTable(tableId);
    if (live) return live;
    const latest = await this.latestForTable(tableId);
    if (!latest) return null;
    const closedAt = latest.closedAt ? Date.parse(latest.closedAt) : null;
    if (closedAt === null || Date.now() - closedAt > RECEIPT_WINDOW_HOURS * 36e5) return null;
    if (!deviceHash2) return null;
    const wasThere = await this.q.one(
      "SELECT 1 AS hit FROM session_participants WHERE session_id = $1 AND device_hash = $2",
      [latest.id, deviceHash2]
    );
    return wasThere ? latest : null;
  }
  /**
   * Tables nobody has touched in a while, with no live checkout outstanding.
   *
   * The second half matters. A diner doing bank authentication on a provider's
   * hosted page sends us nothing for minutes at a time, and abandoning their
   * table would void a checkout that is seconds from being paid.
   */
  async staleSessions(idleMinutes) {
    const rows = await this.q(
      `SELECT ${S_COLS} FROM sessions s
        WHERE s.status = ANY($1)
          AND s.last_activity_at < now() - ($2 || ' minutes')::interval
          AND NOT EXISTS (
            SELECT 1 FROM payments p
             WHERE p.session_id = s.id AND p.status = 'pending' AND p.voided_at IS NULL
          )
        ORDER BY s.last_activity_at`,
      [LIVE_STATUSES, String(idleMinutes)]
    );
    return rows.map(mapSession);
  }
  async byId(id) {
    const row = await this.q.one(`SELECT ${S_COLS} FROM sessions WHERE id = $1`, [id]);
    return row ? mapSession(row) : null;
  }
  async full(id) {
    const session = await this.byId(id);
    if (!session) return null;
    const items = await this.items(id);
    const participants = await this.participants(id);
    const claims = await this.claims(id);
    return { session, items, participants, claims };
  }
  /**
   * Every status change goes through here so the version check is not something
   * a caller can forget. Two staff acting at once on the same table must not be
   * last-write-wins on a value that decides whether money is still owed.
   */
  async setStatus(id, status, expectedVersion) {
    const row = await this.q.one(
      `UPDATE sessions
          SET status = $2, version = version + 1, updated_at = now(), last_activity_at = now(),
              closed_at = CASE WHEN $2 IN ('paid','short','closed','abandoned') THEN now() ELSE closed_at END,
              receipt_until = CASE WHEN $2 IN ('paid','short','closed')
                                   THEN now() + interval '2 hours' ELSE receipt_until END
        WHERE id = $1 AND ($3::int IS NULL OR version = $3)
        RETURNING ${S_COLS}`,
      [id, status, expectedVersion ?? null]
    );
    if (!row) throw new StaleVersionError("That table changed while you were looking at it. Reload and try again.");
    return mapSession(row);
  }
  async touch(id) {
    await this.q("UPDATE sessions SET last_activity_at = now() WHERE id = $1", [id]);
  }
  async closeShort(id, shortCents, reason) {
    const row = await this.q.one(
      `UPDATE sessions
          SET status = 'short', short_cents = $2, short_reason = $3,
              closed_at = now(), version = version + 1, updated_at = now()
        WHERE id = $1 RETURNING ${S_COLS}`,
      [id, shortCents, reason]
    );
    return mapSession(row);
  }
  /* ------------------------------------------------------------- roster -- */
  async participants(sessionId, includeRemoved = false) {
    const rows = await this.q(
      `SELECT ${P_COLS} FROM session_participants
        WHERE session_id = $1 AND ($2 OR removed_at IS NULL)
        ORDER BY joined_at, id`,
      [sessionId, includeRemoved]
    );
    return rows.map(mapParticipant);
  }
  async participantById(id) {
    const row = await this.q.one(`SELECT ${P_COLS} FROM session_participants WHERE id = $1`, [id]);
    return row ? mapParticipant(row) : null;
  }
  /**
   * Identity comes from the bearer token, never from an id in a request body.
   *
   * Expiry is enforced here rather than by a sweeper, so it cannot be
   * forgotten: every authenticated action in the diner plane passes through
   * this one query, and activity refreshes the window in the same statement.
   */
  async identify(sessionId, token) {
    if (!token) return null;
    const row = await this.q.one(
      `UPDATE session_participants
          SET last_seen_at = now(),
              token_expires_at = now() + ($3 || ' hours')::interval
        WHERE session_id = $1
          AND token_hash = $2
          AND removed_at IS NULL
          AND (token_expires_at IS NULL OR token_expires_at > now())
        RETURNING ${P_COLS}`,
      [sessionId, hashToken(token), String(TOKEN_TTL_HOURS)]
    );
    return row ? mapParticipant(row) : null;
  }
  async byDevice(sessionId, deviceHash2) {
    const row = await this.q.one(
      `SELECT ${P_COLS} FROM session_participants
        WHERE session_id = $1 AND device_hash = $2 AND removed_at IS NULL`,
      [sessionId, deviceHash2]
    );
    return row ? mapParticipant(row) : null;
  }
  /**
   * Join, or re-join.
   *
   * A rescan must not add a phantom diner: in equal mode an extra head changes
   * what everyone else owes. The device hash is server-minted and signed, and
   * the uniqueness is a database constraint rather than a read-then-write
   * check, which races on bad restaurant wifi.
   *
   * The first device to arrive becomes host. That is also enforced by a partial
   * unique index, so two phones landing in the same tick cannot both win it.
   */
  async join(sessionId, input) {
    const existing = await this.byDevice(sessionId, input.deviceHash);
    if (existing) {
      const token2 = mintToken();
      await this.q(
        `UPDATE session_participants
            SET token_hash = $2,
                token_expires_at = now() + ($3 || ' hours')::interval,
                last_seen_at = now()
          WHERE id = $1`,
        [existing.id, hashToken(token2), String(TOKEN_TTL_HOURS)]
      );
      return { participant: existing, token: token2, rejoined: true };
    }
    const token = mintToken();
    const roster = await this.participants(sessionId);
    const wantsHost = roster.length === 0;
    try {
      const row = await this.q.one(
        `INSERT INTO session_participants
           (id, restaurant_id, session_id, name, is_host, token_hash, device_hash, seat_no,
            token_expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now() + ($9 || ' hours')::interval)
         RETURNING ${P_COLS}`,
        [
          newId("par"),
          this.restaurantId,
          sessionId,
          input.name?.trim() || null,
          wantsHost,
          hashToken(token),
          input.deviceHash,
          roster.length + 1,
          String(TOKEN_TTL_HOURS)
        ]
      );
      return { participant: mapParticipant(row), token, rejoined: false };
    } catch (err) {
      const msg = String(err.message);
      if (msg.includes("idx_participant_name")) {
        throw new NameTakenError("Someone at this table is already using that name. Add an initial.");
      }
      if (msg.includes("idx_participant_one_host")) {
        return this.join(sessionId, input);
      }
      if (msg.includes("idx_participant_device")) {
        const now = await this.byDevice(sessionId, input.deviceHash);
        if (now) return { participant: now, token, rejoined: true };
      }
      throw err;
    }
  }
  async setName(participantId, name) {
    try {
      const row = await this.q.one(
        `UPDATE session_participants SET name = $2 WHERE id = $1 RETURNING ${P_COLS}`,
        [participantId, name.trim()]
      );
      return row ? mapParticipant(row) : null;
    } catch (err) {
      if (String(err.message).includes("idx_participant_name")) {
        throw new NameTakenError("Someone at this table is already using that name. Add an initial.");
      }
      throw err;
    }
  }
  /**
   * Eject is a soft mark, never a delete.
   *
   * `payments.participant_id` is ON DELETE RESTRICT precisely so this cannot
   * become a way to erase the record that money arrived. Removing someone who
   * has paid is refused by the database rather than silently shrinking the
   * ledger.
   */
  async eject(participantId) {
    await this.q("UPDATE session_participants SET removed_at = now() WHERE id = $1", [participantId]);
  }
  async transferHost(sessionId, toParticipantId) {
    await this.q("UPDATE session_participants SET is_host = FALSE WHERE session_id = $1 AND is_host", [
      sessionId
    ]);
    await this.q("UPDATE session_participants SET is_host = TRUE WHERE id = $1", [toParticipantId]);
  }
  /* --------------------------------------------------------------- bill -- */
  async items(sessionId) {
    const rows = await this.q(
      `SELECT id, position, qty, description, unit_cents, line_cents
         FROM items WHERE session_id = $1 ORDER BY position`,
      [sessionId]
    );
    return rows.map((r) => ({
      id: r.id,
      position: r.position,
      qty: r.qty,
      description: r.description,
      unitCents: r.unit_cents,
      lineCents: r.line_cents
    }));
  }
  async claims(sessionId) {
    const rows = await this.q(
      "SELECT item_id, participant_id FROM claims WHERE session_id = $1",
      [sessionId]
    );
    return rows.map((r) => ({ itemId: r.item_id, participantId: r.participant_id }));
  }
  /**
   * Replace the line list, keeping claims on lines that survive.
   *
   * The previous implementation deleted every item and re-inserted, and because
   * `claims.item_id` cascades, that silently wiped every claim at the table --
   * including for lines resubmitted unchanged. Its own comment said the
   * opposite. In practice a host fixing one mistyped price made all four diners
   * re-tap everything.
   *
   * So this deletes only the lines that actually went away.
   */
  async replaceItems(sessionId, items) {
    const existing = await this.items(sessionId);
    const existingIds = new Set(existing.map((i) => i.id));
    const rows = items.map((raw, index) => {
      const id = raw.id && existingIds.has(raw.id) ? raw.id : newId("itm");
      const qty = Math.max(1, Math.trunc(raw.qty || 1));
      const lineCents = Math.max(0, Math.round(raw.lineCents));
      return {
        id,
        position: index,
        qty,
        description: raw.description.trim().slice(0, 80) || "ITEM",
        unitCents: qty ? Math.round(lineCents / qty) : lineCents,
        lineCents
      };
    }).filter((it) => it.lineCents > 0);
    const keep = rows.map((r) => r.id);
    await this.q(
      `DELETE FROM items WHERE session_id = $1 AND NOT (id = ANY($2::text[]))`,
      [sessionId, keep]
    );
    for (const it of rows) {
      await this.q(
        `INSERT INTO items (id, restaurant_id, session_id, position, qty, description, unit_cents, line_cents)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO UPDATE
            SET position = EXCLUDED.position, qty = EXCLUDED.qty,
                description = EXCLUDED.description, unit_cents = EXCLUDED.unit_cents,
                line_cents = EXCLUDED.line_cents`,
        [it.id, this.restaurantId, sessionId, it.position, it.qty, it.description, it.unitCents, it.lineCents]
      );
    }
    await this.recalcItemsTotal(sessionId);
  }
  async recalcItemsTotal(sessionId) {
    await this.q(
      `UPDATE sessions
          SET items_cents = coalesce((SELECT sum(line_cents) FROM items WHERE session_id = $1), 0),
              updated_at = now()
        WHERE id = $1`,
      [sessionId]
    );
  }
  async setClaim(sessionId, itemId, participantId, claimed) {
    if (claimed) {
      await this.q(
        `INSERT INTO claims (restaurant_id, session_id, item_id, participant_id)
         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
        [this.restaurantId, sessionId, itemId, participantId]
      );
    } else {
      await this.q("DELETE FROM claims WHERE item_id = $1 AND participant_id = $2", [
        itemId,
        participantId
      ]);
    }
  }
  /* ---------------------------------------------------------- money set -- */
  /**
   * Lock the roster and freeze what everyone owes, in one step.
   *
   * These belong together. Freezing the total before the tip is chosen leaves
   * the tip with nowhere to go; locking the roster after the shares are
   * computed lets a late scan change what an already-paid diner owed. So the
   * order is: roster locked, bill confirmed, tip chosen, then total and mode
   * frozen together.
   *
   * `headcount` may exceed the roster (two people sharing one phone) but never
   * fall below it -- that would leave shares with no one to assign them to and
   * make the reconciliation target unreachable.
   */
  async lockAndFreeze(input) {
    const roster = await this.participants(input.sessionId);
    if (input.headcount < roster.length) {
      throw new Error(
        `Headcount ${input.headcount} is below the ${roster.length} people who have joined.`
      );
    }
    const row = await this.q.one(
      `UPDATE sessions
          SET pay_mode = $2,
              headcount = $3,
              headcount_locked_at = now(),
              tip_cents = $4,
              service_cents = $5,
              bill_total_cents = items_cents + $4 + $5,
              bill_frozen_at = now(),
              status = 'awaiting_payment',
              version = version + 1,
              updated_at = now(),
              last_activity_at = now()
        WHERE id = $1 AND ($6::int IS NULL OR version = $6)
        RETURNING ${S_COLS}`,
      [
        input.sessionId,
        input.payMode,
        input.headcount,
        Math.max(0, Math.round(input.tipCents)),
        Math.max(0, Math.round(input.serviceCents)),
        input.expectedVersion ?? null
      ]
    );
    if (!row) throw new StaleVersionError("That table changed while you were looking at it. Reload and try again.");
    return mapSession(row);
  }
  async setUnassignedCents(sessionId, cents) {
    await this.q("UPDATE sessions SET unassigned_cents = $2 WHERE id = $1", [sessionId, cents]);
  }
  /* --------------------------------------------------------------- lists */
  async listRecent(limit = 50) {
    const rows = await this.q(
      `SELECT ${S_COLS} FROM sessions ORDER BY created_at DESC LIMIT $1`,
      [limit]
    );
    return rows.map(mapSession);
  }
  async listLive() {
    const rows = await this.q(
      `SELECT ${S_COLS} FROM sessions WHERE status = ANY($1) ORDER BY created_at`,
      [LIVE_STATUSES]
    );
    return rows.map(mapSession);
  }
};

// apps/server/src/services/receipts.ts
async function buildReceipt(t, payment) {
  const full = await t.sessions.full(payment.sessionId);
  if (!full) return null;
  const table = await t.tables.byId(full.session.tableId);
  const settings = await readSettings(t);
  const payerId = payment.onBehalfOf ?? payment.participantId;
  const payer = full.participants.find((p) => p.id === payerId);
  const lines = full.session.payMode === "items" ? full.items.filter((item) => full.claims.some((c) => c.itemId === item.id && c.participantId === payerId)).map((item) => ({ description: item.description, qty: item.qty, lineCents: item.lineCents })) : [];
  return {
    restaurantName: settings.displayName,
    tableCode: table?.code ?? "?",
    currency: full.session.currency,
    reference: payment.reference,
    paidAtIso: payment.paidAt ?? (/* @__PURE__ */ new Date()).toISOString(),
    amountCents: payment.receivedCents ?? payment.expectedCents,
    payerName: (payer?.name ?? "").trim() || `Guest ${payer?.seatNo ?? ""}`.trim(),
    billTotalCents: full.session.billTotalCents ?? 0,
    tipCents: full.session.tipCents,
    serviceCents: full.session.serviceCents,
    lines,
    closingNote: settings.closingNote
  };
}
function receiptHtml(r) {
  const money = (c) => formatMoney(c, r.currency);
  const when = new Date(r.paidAtIso);
  const lines = r.lines.map(
    (l) => `<tr><td>${escapeHtml(l.qty > 1 ? `${l.qty}\xD7 ${l.description}` : l.description)}</td><td class="r">${escapeHtml(money(l.lineCents))}</td></tr>`
  ).join("");
  return `<!doctype html>
<meta charset="utf-8">
<title>Receipt ${escapeHtml(r.reference)}</title>
<style>
  @page { size: 80mm auto; margin: 3mm; }
  body {
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    font-size: 12px; line-height: 1.45; color: #000; background: #fff;
    margin: 0 auto; max-width: 72mm; padding: 4mm 0;
  }
  h1 { font-size: 14px; text-align: center; margin: 0 0 2mm; letter-spacing: .04em; }
  .meta { text-align: center; margin-bottom: 3mm; }
  hr { border: 0; border-top: 1px dashed #000; margin: 2mm 0; }
  table { width: 100%; border-collapse: collapse; }
  td { padding: .4mm 0; vertical-align: top; }
  .r { text-align: right; white-space: nowrap; padding-left: 3mm; }
  .total td { font-weight: 700; font-size: 13px; padding-top: 1mm; }
  .note { text-align: center; margin-top: 3mm; }
  .ref { font-size: 10px; word-break: break-all; text-align: center; margin-top: 3mm; }
  @media screen {
    body { box-shadow: 0 0 0 1px #e2ded6; padding: 6mm; margin-top: 2rem; }
    .print { display: block; width: 100%; margin: 1rem auto 0; max-width: 72mm;
             padding: .6rem; font: inherit; cursor: pointer; }
  }
  @media print { .print { display: none; } }
</style>
<h1>${escapeHtml(r.restaurantName)}</h1>
<div class="meta">
  Table ${escapeHtml(r.tableCode)}<br>
  ${escapeHtml(when.toLocaleString())}
</div>
<hr>
${lines ? `<table>${lines}</table><hr>` : ""}
<table>
  ${r.serviceCents > 0 ? `<tr><td>Service</td><td class="r">${escapeHtml(money(r.serviceCents))}</td></tr>` : ""}
  ${r.tipCents > 0 ? `<tr><td>Tip</td><td class="r">${escapeHtml(money(r.tipCents))}</td></tr>` : ""}
  <tr><td>Table total</td><td class="r">${escapeHtml(money(r.billTotalCents))}</td></tr>
  <tr class="total"><td>Paid by ${escapeHtml(r.payerName)}</td><td class="r">${escapeHtml(money(r.amountCents))}</td></tr>
</table>
<hr>
${r.closingNote ? `<div class="note">${escapeHtml(r.closingNote)}</div>` : ""}
<div class="ref">${escapeHtml(r.reference)}</div>
<button class="print" onclick="window.print()">Print</button>`;
}
function receiptText(r) {
  const money = (c) => formatMoney(c, r.currency);
  const parts = [
    r.restaurantName,
    `Table ${r.tableCode} \xB7 ${new Date(r.paidAtIso).toLocaleString()}`,
    ""
  ];
  if (r.lines.length > 0) {
    for (const l of r.lines) {
      parts.push(`${l.qty > 1 ? `${l.qty}x ` : ""}${l.description}  ${money(l.lineCents)}`);
    }
    parts.push("");
  }
  if (r.serviceCents > 0) parts.push(`Service      ${money(r.serviceCents)}`);
  if (r.tipCents > 0) parts.push(`Tip          ${money(r.tipCents)}`);
  parts.push(`Table total  ${money(r.billTotalCents)}`);
  parts.push(`You paid     ${money(r.amountCents)}`);
  parts.push("");
  if (r.closingNote) parts.push(r.closingNote, "");
  parts.push(`Reference: ${r.reference}`);
  return parts.join("\n");
}
function escapeHtml(s) {
  return s.replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]
  );
}

// apps/server/src/routes/table.ts
function tableRoutes(ctx) {
  const router = Router({ mergeParams: true });
  async function inTable(req, fn, opts = {}) {
    const slug = String(req.params.slug ?? "");
    const code = String(req.params.code ?? "");
    const restaurant = await ctx.registry.bySlug(slug);
    if (!restaurant) {
      throttleMiss(req, slug, code);
      throw ApiError.notFound("This table is not open yet. Ask your server to start it.");
    }
    const pending = [];
    let channel = null;
    const out = await ctx.tenant(restaurant.id, async (t) => {
      const table = await t.tables.byCode(code);
      const session = table ? opts.allowClosed ? await t.sessions.readableForTable(table.id, readDevice(req, ctx.appKey).hash) : await t.sessions.liveForTable(table.id) : null;
      if (!session) return null;
      channel = channelFor(t.restaurantId, session.id);
      return fn({
        t,
        session,
        emit: (event, data) => pending.push({ event, data })
      });
    });
    if (out === null && pending.length === 0 && channel === null) {
      throttleMiss(req, slug, code);
      throw ApiError.notFound("This table is not open yet. Ask your server to start it.");
    }
    for (const p of pending) ctx.events.broadcast(channel, p.event, p.data ?? {});
    return out;
  }
  function throttleMiss(req, slug, code) {
    const ip = req.ip ?? "unknown";
    const byIp = ctx.limiter.hit(
      tenantKey(slug, "code-miss", ip),
      LIMITS.codeMiss.limit,
      LIMITS.codeMiss.windowMs
    );
    const byTable = ctx.limiter.hit(
      tenantKey(slug, "code-miss-table", code),
      LIMITS.joinPerTable.limit,
      LIMITS.joinPerTable.windowMs
    );
    if (!byIp.allowed || !byTable.allowed) {
      throw ApiError.tooMany("Too many attempts. Wait a minute.");
    }
  }
  async function member(l, req) {
    const header2 = req.get("authorization") ?? "";
    const token = header2.startsWith("Bearer ") ? header2.slice(7).trim() : null;
    const me = await l.t.sessions.identify(l.session.id, token);
    if (!me) throw ApiError.unauthorized("Join this table first.");
    return me;
  }
  async function host(l, req) {
    const me = await member(l, req);
    if (!me.isHost) throw ApiError.forbidden("Only the host can do that.");
    return me;
  }
  const view = async (l, viewerId) => publicView(l.t, await l.t.sessions.full(l.session.id), viewerId);
  router.get("/", async (req, res) => {
    const out = await inTable(req, async (l) => {
      const device = readDevice(req, ctx.appKey);
      const me = await l.t.sessions.byDevice(l.session.id, device.hash);
      return { session: await view(l, me?.id ?? null), joined: me !== null };
    }, { allowClosed: true });
    res.json({ ok: true, ...out });
  });
  router.post("/join", async (req, res) => {
    const body = joinSessionSchema.parse(req.body ?? {});
    const device = readDevice(req, ctx.appKey);
    const out = await inTable(req, async (l) => {
      const already = await l.t.sessions.byDevice(l.session.id, device.hash);
      if (!already && l.session.headcountLockedAt !== null) {
        throw ApiError.conflict("This table is already counted. Ask the host to add you.");
      }
      let result;
      try {
        result = await l.t.sessions.join(l.session.id, {
          deviceHash: device.hash,
          name: body.name ?? null
        });
      } catch (err) {
        if (err instanceof NameTakenError) throw ApiError.conflict(err.message);
        throw err;
      }
      l.emit("roster");
      return {
        token: result.token,
        participantId: result.participant.id,
        isHost: result.participant.isHost,
        rejoined: result.rejoined,
        session: await view(l, result.participant.id)
      };
    });
    setDeviceCookie(res, device, ctx.appKey, req.secure);
    res.json({ ok: true, ...out });
  });
  router.post("/name", async (req, res) => {
    const body = setNameSchema.parse(req.body);
    const out = await inTable(req, async (l) => {
      const me = await member(l, req);
      try {
        await l.t.sessions.setName(me.id, body.name);
      } catch (err) {
        if (err instanceof NameTakenError) throw ApiError.conflict(err.message);
        throw err;
      }
      l.emit("roster");
      return { session: await view(l, me.id) };
    });
    res.json({ ok: true, ...out });
  });
  router.post("/eject", async (req, res) => {
    const body = ejectSchema.parse(req.body);
    await inTable(req, async (l) => {
      const me = await host(l, req);
      if (body.participantId === me.id) throw ApiError.badRequest("You cannot remove yourself.");
      if (l.session.headcountLockedAt) {
        throw ApiError.conflict("The table is already counted. Ask your server to change it.");
      }
      await l.t.sessions.eject(body.participantId);
      l.emit("roster");
      return null;
    });
    res.json({ ok: true });
  });
  router.post("/host", async (req, res) => {
    const body = transferHostSchema.parse(req.body);
    await inTable(req, async (l) => {
      await host(l, req);
      await l.t.sessions.transferHost(l.session.id, body.participantId);
      l.emit("roster");
      return null;
    });
    res.json({ ok: true });
  });
  router.put("/bill", async (req, res) => {
    const body = updateBillSchema.parse(req.body);
    const out = await inTable(req, async (l) => {
      const me = await host(l, req);
      await l.t.sessions.replaceItems(l.session.id, body.items);
      const voided = await invalidateCheckouts(l.t, l.session);
      l.emit("bill", { voided });
      return { session: await view(l, me.id) };
    });
    res.json({ ok: true, ...out });
  });
  router.post("/claim", async (req, res) => {
    const body = claimSchema.parse(req.body);
    const out = await inTable(req, async (l) => {
      const me = await member(l, req);
      const target = body.forParticipantId ?? me.id;
      if (target !== me.id) {
        if (!me.isHost) throw ApiError.forbidden("Only the host can assign a line to someone else.");
        await l.t.telemetry.audit({
          actorType: "participant",
          actorId: me.id,
          action: "claim.on_behalf",
          sessionId: l.session.id,
          targetId: target,
          meta: { itemId: body.itemId, claimed: body.claimed }
        });
      }
      await l.t.sessions.setClaim(l.session.id, body.itemId, target, body.claimed);
      l.emit("bill");
      return { session: await view(l, me.id) };
    });
    res.json({ ok: true, ...out });
  });
  router.post("/lock", async (req, res) => {
    const body = lockRosterSchema.parse(req.body);
    const out = await inTable(req, async (l) => {
      const me = await host(l, req);
      try {
        await l.t.sessions.lockAndFreeze({
          sessionId: l.session.id,
          payMode: body.payMode,
          headcount: body.headcount,
          tipCents: body.tipCents,
          serviceCents: body.serviceCents,
          expectedVersion: body.version
        });
      } catch (err) {
        if (err instanceof StaleVersionError) throw ApiError.conflict(err.message);
        if (err instanceof ApiError) throw err;
        throw ApiError.badRequest(err.message);
      }
      l.emit("bill");
      return { session: await view(l, me.id) };
    });
    res.json({ ok: true, ...out });
  });
  router.post("/scan", async (req, res) => {
    const body = scanBillSchema.parse(req.body);
    const out = await inTable(req, async (l) => {
      const me = await host(l, req);
      const gate = ctx.limiter.hit(
        tenantKey(l.t.restaurantId, "scan", l.session.id),
        LIMITS.scan.limit,
        LIMITS.scan.windowMs
      );
      if (!gate.allowed) throw ApiError.tooMany("Too many scans. Give it a minute.");
      if (l.session.billFrozenAt) {
        throw ApiError.conflict("The bill is already finalised. Ask your server to reopen it.");
      }
      const vision = new VisionService(
        { ...await resolveVision(ctx, l.t), pricing: DEFAULT_PRICING },
        l.t.telemetry
      );
      let scanned;
      try {
        scanned = await vision.scanBill({
          imageBase64: body.imageBase64,
          mediaType: body.mediaType,
          sessionId: l.session.id
        });
      } catch (err) {
        if (err instanceof VisionNotConfigured) {
          throw ApiError.badRequest(
            "This restaurant has not set up bill scanning. Enter the items by hand."
          );
        }
        if (err instanceof VisionRefused || err instanceof VisionError) {
          throw ApiError.badRequest(err.message);
        }
        throw err;
      }
      await l.t.sessions.replaceItems(
        l.session.id,
        scanned.items.map((i) => ({ qty: i.qty, description: i.description, lineCents: i.lineCents }))
      );
      await invalidateCheckouts(l.t, l.session);
      l.emit("bill", {});
      return {
        session: await view(l, me.id),
        read: {
          confidence: scanned.confidence,
          notes: scanned.notes,
          mismatch: scanned.mismatch,
          serviceCents: scanned.serviceCents,
          statedTotalCents: scanned.statedTotalCents
        }
      };
    });
    res.json({ ok: true, ...out });
  });
  router.post("/checkout", async (req, res) => {
    const body = checkoutSchema.parse(req.body ?? {});
    const origin = ctx.publicUrl ?? `${req.protocol}://${req.get("host") ?? "localhost:3000"}`;
    const out = await inTable(req, async (l) => {
      const me = await member(l, req);
      const table = await l.t.tables.byId(l.session.tableId);
      const tableUrl2 = `${origin}/${l.t.restaurant.slug}/t/${encodeURIComponent(table?.code ?? "")}`;
      let psp;
      try {
        psp = await resolvePsp(ctx, l.t, {
          returnUrl: `${tableUrl2}?paid=1`,
          cancelUrl: tableUrl2,
          webhookUrl: `${origin}/api/webhooks/psp/${l.t.restaurant.webhookToken}`
        });
      } catch (err) {
        if (err instanceof PspNotConfigured || err instanceof PspCredentialUnreadable || err instanceof ProviderConfigError) {
          await l.t.telemetry.audit({
            actorType: "system",
            action: "psp.unavailable",
            sessionId: l.session.id,
            meta: { reason: err.message }
          });
          throw ApiError.badRequest(
            "This restaurant has not finished setting up payments. Ask your server."
          );
        }
        throw err;
      }
      try {
        const { payment, amountCents } = await raiseCheckout(l.t, psp, {
          sessionId: l.session.id,
          tableCode: table?.code ?? "",
          payerId: me.id,
          onBehalfOf: body.onBehalfOf ?? null,
          coverUnassigned: body.coverUnassigned ?? false,
          origin
        });
        return {
          reference: payment.reference,
          authorizationUrl: payment.authorizationUrl,
          amountCents,
          provider: psp.provider.id,
          // Said out loud to the diner. Nobody should be unsure whether they
          // actually paid.
          mock: psp.provider.simulated
        };
      } catch (err) {
        if (err instanceof UnassignedSharesError) throw ApiError.conflict(err.message);
        if (err instanceof WrongModeError) throw ApiError.forbidden(err.message);
        if (err instanceof BillNotFrozenError) throw ApiError.conflict(err.message);
        throw err;
      }
    });
    res.json({ ok: true, ...out });
  });
  router.post("/receipt", async (req, res) => {
    const to = String(req.body.email ?? "").trim();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(to)) {
      throw ApiError.badRequest("That does not look like an email address.");
    }
    const message = await inTable(
      req,
      async (l) => {
        const me = await member(l, req);
        if (!ctx.limiter.hit(tenantKey(l.t.restaurantId, "receipt", me.id), 5, 36e5).allowed) {
          throw ApiError.tooMany("That receipt has been sent a few times already.");
        }
        const payments = await l.t.payments.forSession(l.session.id);
        const mine = payments.find(
          (p) => (p.onBehalfOf ?? p.participantId) === me.id && p.status === "paid"
        );
        if (!mine) throw ApiError.conflict("There is no completed payment for you at this table.");
        const receipt = await buildReceipt(l.t, mine);
        if (!receipt) return null;
        return {
          to,
          subject: `Your receipt from ${receipt.restaurantName}`,
          text: receiptText(receipt)
        };
      },
      { allowClosed: true }
    );
    if (!message) throw ApiError.notFound("No such payment.");
    const delivery = await ctx.mail.send(message);
    res.json({ ok: true, sent: delivery.sent, ...delivery.fallbackNotice ? { notice: delivery.fallbackNotice } : {} });
  });
  router.get("/events", async (req, res) => {
    const token = req.query.token ?? null;
    const channel = await inTable(req, async (l) => {
      const me = await l.t.sessions.identify(l.session.id, token);
      if (!me) throw ApiError.unauthorized("Join this table first.");
      return channelFor(l.t.restaurantId, l.session.id);
    }, { allowClosed: true });
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
      "x-accel-buffering": "no"
    });
    res.write(": connected\n\n");
    const unsubscribe = ctx.events.subscribe(channel, res);
    req.on("close", unsubscribe);
  });
  return router;
}
async function settleAndBroadcast(ctx, t, sessionId) {
  const settled = await maybeSettle(t, sessionId);
  const channel = channelFor(t.restaurantId, sessionId);
  ctx.events.broadcast(channel, "payments", {});
  if (settled) ctx.events.broadcast(channel, "settled", {});
}

// apps/server/src/routes/staff.ts
import { Router as Router2 } from "express";

// apps/server/src/db/tables.ts
function map2(r) {
  return {
    id: r.id,
    code: r.code,
    label: r.label ?? null,
    seats: r.seats,
    createdAt: isoRequired(r.created_at),
    archivedAt: iso(r.archived_at)
  };
}
var COLUMNS2 = "id, code, label, seats, created_at, archived_at";
var CodeTakenError = class extends Error {
};
var CodeRetiredError = class extends Error {
};
var TableRepository = class {
  constructor(q, restaurantId) {
    this.q = q;
    this.restaurantId = restaurantId;
  }
  async list(includeArchived = false) {
    const rows = await this.q(
      `SELECT ${COLUMNS2} FROM tables
        WHERE ($1 OR archived_at IS NULL)
        ORDER BY code`,
      [includeArchived]
    );
    return rows.map(map2);
  }
  async byId(id) {
    const row = await this.q.one(`SELECT ${COLUMNS2} FROM tables WHERE id = $1`, [id]);
    return row ? map2(row) : null;
  }
  async byCode(code) {
    const row = await this.q.one(
      `SELECT ${COLUMNS2} FROM tables WHERE code = $1 AND archived_at IS NULL`,
      [code.trim()]
    );
    return row ? map2(row) : null;
  }
  async create(input) {
    const code = input.code.trim();
    await this.assertCodeFree(code, null);
    try {
      const row = await this.q.one(
        `INSERT INTO tables (id, restaurant_id, code, label, seats)
         VALUES ($1, $2, $3, $4, coalesce($5, 4))
         RETURNING ${COLUMNS2}`,
        [newId("tb"), this.restaurantId, code, input.label ?? null, input.seats ?? null]
      );
      return map2(row);
    } catch (err) {
      throw translate(err, code);
    }
  }
  /**
   * Changing a code retires the old one. The caller is responsible for warning
   * that stickers need reprinting -- this only guarantees the old code can
   * never point somewhere new.
   */
  async update(id, input) {
    const existing = await this.byId(id);
    if (!existing) return null;
    const code = input.code?.trim() ?? existing.code;
    if (code.toLowerCase() !== existing.code.toLowerCase()) {
      await this.assertCodeFree(code, id);
      await this.q(
        `INSERT INTO retired_table_codes (restaurant_id, code, table_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (restaurant_id, code) DO NOTHING`,
        [this.restaurantId, existing.code, id]
      );
    }
    try {
      const row = await this.q.one(
        `UPDATE tables SET code = $2, label = $3, seats = coalesce($4, seats)
          WHERE id = $1 RETURNING ${COLUMNS2}`,
        [id, code, input.label ?? existing.label, input.seats ?? null]
      );
      return row ? map2(row) : null;
    } catch (err) {
      throw translate(err, code);
    }
  }
  /** Archived, not deleted: sessions reference tables, and history is money. */
  async archive(id) {
    await this.q("UPDATE tables SET archived_at = now() WHERE id = $1 AND archived_at IS NULL", [id]);
  }
  async assertCodeFree(code, exceptTableId) {
    const live = await this.q.one(
      "SELECT id FROM tables WHERE code = $1 AND ($2::text IS NULL OR id <> $2)",
      [code, exceptTableId]
    );
    if (live) throw new CodeTakenError(`Table ${code} already exists.`);
    const retired = await this.q.one(
      "SELECT table_id FROM retired_table_codes WHERE code = $1",
      [code]
    );
    if (retired && retired.table_id !== exceptTableId) {
      throw new CodeRetiredError(
        `Code ${code} was used by another table and cannot be reused -- printed QR codes may still point at it.`
      );
    }
  }
};
function translate(err, code) {
  const message = String(err.message);
  if (message.includes("tables_restaurant_id_code_key")) return new CodeTakenError(`Table ${code} already exists.`);
  return err;
}

// apps/server/src/db/staff.ts
var U_COLS = `id, email, display_name, role, totp_secret_enc, created_at, last_login_at, disabled_at`;
function mapUser(r) {
  return {
    id: r.id,
    email: r.email,
    displayName: r.display_name ?? null,
    role: r.role,
    hasTotp: r.totp_secret_enc !== null && r.totp_secret_enc !== void 0,
    createdAt: isoRequired(r.created_at),
    lastLoginAt: iso(r.last_login_at),
    disabledAt: iso(r.disabled_at)
  };
}
var DUMMY_HASH = hashPassword("sharyt-timing-equaliser");
var EmailTakenError = class extends Error {
};
var StaffRepository = class {
  constructor(q, restaurantId) {
    this.q = q;
    this.restaurantId = restaurantId;
  }
  async count() {
    const row = await this.q.one("SELECT count(*)::int AS n FROM restaurant_users");
    return row?.n ?? 0;
  }
  async list() {
    const rows = await this.q(`SELECT ${U_COLS} FROM restaurant_users ORDER BY created_at`);
    return rows.map(mapUser);
  }
  async byId(id) {
    const row = await this.q.one(`SELECT ${U_COLS} FROM restaurant_users WHERE id = $1`, [id]);
    return row ? mapUser(row) : null;
  }
  async create(input) {
    try {
      const row = await this.q.one(
        `INSERT INTO restaurant_users (id, restaurant_id, email, display_name, password_hash, role)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING ${U_COLS}`,
        [
          newId("usr"),
          this.restaurantId,
          input.email.trim(),
          input.displayName ?? null,
          hashPassword(input.password),
          input.role
        ]
      );
      return mapUser(row);
    } catch (err) {
      if (String(err.message).includes("restaurant_users_restaurant_id_email_key")) {
        throw new EmailTakenError("Someone here already uses that email address.");
      }
      throw err;
    }
  }
  /**
   * Verify a password within this restaurant.
   *
   * Scoped by construction: the same person may hold accounts at two
   * restaurants, and a credential check that ignored the tenant would let one
   * restaurant's password open another's portal.
   */
  async verifyCredentials(email, password) {
    const row = await this.q.one(
      `SELECT ${U_COLS}, password_hash FROM restaurant_users WHERE email = $1`,
      [email.trim()]
    );
    if (!row) {
      verifyPassword(password, DUMMY_HASH);
      return null;
    }
    if (!verifyPassword(password, row.password_hash)) return null;
    if (row.disabled_at) return null;
    return mapUser(row);
  }
  async recordLogin(userId) {
    await this.q("UPDATE restaurant_users SET last_login_at = now() WHERE id = $1", [userId]);
  }
  async setTotpSecret(userId, envelope) {
    await this.q("UPDATE restaurant_users SET totp_secret_enc = $2 WHERE id = $1", [userId, envelope]);
  }
  async totpSecretEnvelope(userId) {
    const row = await this.q.one(
      "SELECT totp_secret_enc FROM restaurant_users WHERE id = $1",
      [userId]
    );
    return row?.totp_secret_enc ?? null;
  }
  /* ------------------------------------------------------------ sessions */
  async createSession(input) {
    const token = mintToken();
    const row = await this.q.one(
      `INSERT INTO restaurant_sessions
         (id, restaurant_id, user_id, token_hash, expires_at, ip, user_agent)
       VALUES ($1, $2, $3, $4, now() + ($5 || ' hours')::interval, $6, $7)
       RETURNING id, user_id, created_at, last_seen_at, expires_at, ip, user_agent`,
      [
        newId("ses"),
        this.restaurantId,
        input.userId,
        hashToken(token),
        String(input.ttlHours),
        input.ip ?? null,
        input.userAgent ?? null
      ]
    );
    return { session: mapSession2(row), token };
  }
  /** Resolve a session cookie, refreshing last-seen. Expired or revoked is null. */
  async resolveSession(token) {
    const row = await this.q.one(
      `UPDATE restaurant_sessions s
          SET last_seen_at = now()
        WHERE s.token_hash = $1 AND s.expires_at > now() AND s.revoked_at IS NULL
        RETURNING s.id, s.user_id, s.created_at, s.last_seen_at, s.expires_at, s.ip, s.user_agent`,
      [hashToken(token)]
    );
    if (!row) return null;
    const user = await this.byId(row.user_id);
    if (!user || user.disabledAt) return null;
    return { user, session: mapSession2(row) };
  }
  async sessionsFor(userId) {
    const rows = await this.q(
      `SELECT id, user_id, created_at, last_seen_at, expires_at, ip, user_agent
         FROM restaurant_sessions
        WHERE user_id = $1 AND revoked_at IS NULL AND expires_at > now()
        ORDER BY last_seen_at DESC`,
      [userId]
    );
    return rows.map(mapSession2);
  }
  /**
   * Revoke by id.
   *
   * Scoped, which the previous implementation was not: it let any owner revoke
   * any session id, checked against nothing. Once there is more than one
   * restaurant, that is one tenant force-logging-out another's staff.
   */
  async revokeSession(sessionId) {
    const rows = await this.q(
      "UPDATE restaurant_sessions SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL RETURNING id",
      [sessionId]
    );
    return rows.length > 0;
  }
  async revokeByToken(token) {
    await this.q("UPDATE restaurant_sessions SET revoked_at = now() WHERE token_hash = $1", [
      hashToken(token)
    ]);
  }
};
function mapSession2(r) {
  return {
    id: r.id,
    userId: r.user_id,
    createdAt: isoRequired(r.created_at),
    lastSeenAt: isoRequired(r.last_seen_at),
    expiresAt: isoRequired(r.expires_at),
    ip: r.ip ?? null,
    userAgent: r.user_agent ?? null
  };
}

// apps/server/src/services/mail.ts
import nodemailer from "nodemailer";
var SMTP_KEYS = {
  host: "smtp.host",
  port: "smtp.port",
  secure: "smtp.secure",
  user: "smtp.user",
  password: "smtp.password",
  fromName: "smtp.from_name",
  fromAddress: "smtp.from_address"
};
var MailService = class {
  constructor(db, appKey) {
    this.db = db;
    this.appKey = appKey;
  }
  cached = null;
  async settings() {
    const rows = await this.db.withRegistry(
      (q) => q(
        "SELECT key, value, is_secret FROM platform_settings WHERE key LIKE $1",
        ["smtp.%"]
      )
    );
    const map4 = new Map(rows.map((r) => [r.key, r]));
    const host = map4.get(SMTP_KEYS.host)?.value?.trim();
    const fromAddress = map4.get(SMTP_KEYS.fromAddress)?.value?.trim();
    if (!host || !fromAddress) return null;
    const passwordRow = map4.get(SMTP_KEYS.password);
    let password = "";
    if (passwordRow) {
      try {
        password = decryptSecret(passwordRow.value, this.appKey);
      } catch {
        throw new Error("Could not decrypt the SMTP password. Re-enter it in Sentinel.");
      }
    }
    return {
      host,
      port: Number(map4.get(SMTP_KEYS.port)?.value ?? 587),
      secure: map4.get(SMTP_KEYS.secure)?.value === "true",
      user: map4.get(SMTP_KEYS.user)?.value ?? "",
      password,
      fromName: map4.get(SMTP_KEYS.fromName)?.value ?? "Sharyt",
      fromAddress
    };
  }
  async isConfigured() {
    return await this.settings() !== null;
  }
  async send(message) {
    const smtp = await this.settings();
    if (!smtp) {
      console.log(
        [
          "",
          "\u2500\u2500\u2500 email (no SMTP configured, not sent) \u2500\u2500\u2500",
          `to:      ${message.to}`,
          `subject: ${message.subject}`,
          "",
          message.text,
          "\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500",
          ""
        ].join("\n")
      );
      return {
        sent: false,
        fallbackNotice: "No mail server is configured yet, so the link was written to the server log instead."
      };
    }
    const key = `${smtp.host}:${smtp.port}:${smtp.user}`;
    if (this.cached?.key !== key) {
      this.cached = {
        key,
        transport: nodemailer.createTransport({
          host: smtp.host,
          port: smtp.port,
          secure: smtp.secure,
          auth: smtp.user ? { user: smtp.user, pass: smtp.password } : void 0
        })
      };
    }
    await this.cached.transport.sendMail({
      from: `"${smtp.fromName}" <${smtp.fromAddress}>`,
      to: message.to,
      subject: message.subject,
      text: message.text
    });
    return { sent: true };
  }
  /** Prove the settings work before trusting them with a verification link. */
  async verifyConnection() {
    const smtp = await this.settings();
    if (!smtp) return { ok: false, reason: "No SMTP host or from-address set." };
    try {
      const transport = nodemailer.createTransport({
        host: smtp.host,
        port: smtp.port,
        secure: smtp.secure,
        auth: smtp.user ? { user: smtp.user, pass: smtp.password } : void 0,
        connectionTimeout: 1e4
      });
      await transport.verify();
      return { ok: true };
    } catch (err) {
      return { ok: false, reason: err.message };
    }
  }
  async saveSettings(values) {
    for (const [field, key] of Object.entries(SMTP_KEYS)) {
      const raw = values[field];
      if (raw === void 0 || raw.trim() === "") continue;
      const secret = field === "password";
      const stored = secret ? encryptSecret(raw.trim(), this.appKey) : raw.trim();
      await this.db.withRegistry(
        (q) => q(
          `INSERT INTO platform_settings (key, value, is_secret, updated_at)
           VALUES ($1, $2, $3, now())
           ON CONFLICT (key) DO UPDATE
             SET value = EXCLUDED.value, is_secret = EXCLUDED.is_secret, updated_at = now()`,
          [key, stored, secret]
        )
      );
    }
    this.cached = null;
  }
};
function verifyEmailMessage(input) {
  return {
    to: input.to,
    subject: `Confirm your email for ${input.restaurantName}`,
    text: [
      `Almost there.`,
      ``,
      `Confirm this address to finish setting up ${input.restaurantName} on Sharyt:`,
      ``,
      input.link,
      ``,
      `The link works once and expires in 24 hours.`,
      ``,
      `If you did not sign up, ignore this \u2014 nothing has been created in your name`,
      `that anyone can use.`
    ].join("\n")
  };
}
function resetPasswordMessage(input) {
  return {
    to: input.to,
    subject: "Reset your Sharyt password",
    text: [
      `Someone asked to reset the password for this address.`,
      ``,
      input.link,
      ``,
      `The link works once and expires in one hour.`,
      ``,
      `If it was not you, ignore this. Your password has not changed.`
    ].join("\n")
  };
}

// apps/server/src/services/accounts.ts
var MFA_GRACE_LOGINS = 7;
var VERIFY_TTL_HOURS = 24;
var RESET_TTL_HOURS = 1;
async function issueToken(ctx, input) {
  const token = mintToken();
  await ctx.db.withRegistry(
    (q) => q(
      `INSERT INTO auth_tokens (id, restaurant_id, user_id, purpose, token_hash, expires_at)
       VALUES ($1, $2, $3, $4, $5, now() + ($6 || ' hours')::interval)`,
      [newId("tok"), input.restaurantId, input.userId, input.purpose, hashToken(token), String(input.ttlHours)]
    )
  );
  return token;
}
async function consumeToken(ctx, token, purpose) {
  const row = await ctx.db.withRegistry(
    (q) => q.one(
      `UPDATE auth_tokens
          SET consumed_at = now()
        WHERE token_hash = $1
          AND purpose = $2
          AND consumed_at IS NULL
          AND expires_at > now()
        RETURNING restaurant_id, user_id`,
      [hashToken(token), purpose]
    )
  );
  return row ? { restaurantId: row.restaurant_id, userId: row.user_id } : null;
}
async function sendVerification(ctx, input) {
  const token = await issueToken(ctx, {
    restaurantId: input.restaurantId,
    userId: input.userId,
    purpose: "verify_email",
    ttlHours: VERIFY_TTL_HOURS
  });
  const base = ctx.publicUrl ?? "http://localhost:3000";
  return ctx.mail.send(
    verifyEmailMessage({
      to: input.email,
      restaurantName: input.restaurantName,
      link: `${base}/admin?verify=${encodeURIComponent(token)}`
    })
  );
}
async function verifyEmail(ctx, token) {
  const claim = await consumeToken(ctx, token, "verify_email");
  if (!claim?.restaurantId) return false;
  await ctx.tenant(
    claim.restaurantId,
    (t) => t.q("UPDATE restaurant_users SET email_verified_at = now() WHERE id = $1", [claim.userId])
  );
  return true;
}
async function requestPasswordReset(ctx, input) {
  const restaurant = await ctx.registry.bySlug(input.slug);
  if (!restaurant) return { sent: false };
  const user = await ctx.tenant(
    restaurant.id,
    (t) => t.q.one(
      "SELECT id, email FROM restaurant_users WHERE email = $1 AND disabled_at IS NULL",
      [input.email.trim()]
    )
  );
  if (!user) return { sent: false };
  const token = await issueToken(ctx, {
    restaurantId: restaurant.id,
    userId: user.id,
    purpose: "reset_password",
    ttlHours: RESET_TTL_HOURS
  });
  const base = ctx.publicUrl ?? "http://localhost:3000";
  return ctx.mail.send(
    resetPasswordMessage({
      to: user.email,
      link: `${base}/admin?reset=${encodeURIComponent(token)}`
    })
  );
}
async function completePasswordReset(ctx, token, newPassword) {
  const claim = await consumeToken(ctx, token, "reset_password");
  if (!claim?.restaurantId) return false;
  await ctx.tenant(claim.restaurantId, async (t) => {
    await t.q(
      `UPDATE restaurant_users
          SET password_hash = $2,
              must_change_password = FALSE,
              -- Somebody who can read the address can also prove it.
              email_verified_at = coalesce(email_verified_at, now())
        WHERE id = $1`,
      [claim.userId, hashPassword(newPassword)]
    );
    await t.q("UPDATE restaurant_sessions SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL", [
      claim.userId
    ]);
  });
  return true;
}
async function changePassword(t, userId, newPassword) {
  await t.q(
    "UPDATE restaurant_users SET password_hash = $2, must_change_password = FALSE WHERE id = $1",
    [userId, hashPassword(newPassword)]
  );
}
async function evaluateLogin(t, userId) {
  const row = await t.q.one(
    `UPDATE restaurant_users
        SET logins_without_mfa =
              CASE WHEN totp_secret_enc IS NULL THEN logins_without_mfa + 1 ELSE 0 END
      WHERE id = $1
      RETURNING must_change_password,
                logins_without_mfa,
                (totp_secret_enc IS NOT NULL) AS has_mfa`,
    [userId]
  );
  const count = row?.logins_without_mfa ?? 0;
  const hasMfa = row?.has_mfa ?? false;
  return {
    mustChangePassword: row?.must_change_password ?? false,
    mfaSuggested: !hasMfa && count <= MFA_GRACE_LOGINS,
    // Strictly greater: seven sign-ins may decline, the eighth may not.
    mfaRequired: !hasMfa && count > MFA_GRACE_LOGINS,
    loginsWithoutMfa: count,
    graceRemaining: hasMfa ? 0 : Math.max(0, MFA_GRACE_LOGINS - count + 1)
  };
}
async function isVerified(t, userId) {
  const row = await t.q.one(
    "SELECT (email_verified_at IS NOT NULL) AS verified FROM restaurant_users WHERE id = $1",
    [userId]
  );
  return row?.verified ?? false;
}

// apps/server/src/services/totp.ts
import crypto2 from "node:crypto";
var BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
var PERIOD_SECONDS = 30;
var DIGITS = 6;
function generateTotpSecret(bytes = 20) {
  return base32Encode(crypto2.randomBytes(bytes));
}
function base32Encode(buf) {
  let bits = 0;
  let value = 0;
  let out = "";
  for (const byte of buf) {
    value = value << 8 | byte;
    bits += 8;
    while (bits >= 5) {
      out += BASE32[value >>> bits - 5 & 31];
      bits -= 5;
    }
  }
  if (bits > 0) out += BASE32[value << 5 - bits & 31];
  return out;
}
function base32Decode(input) {
  const clean = input.toUpperCase().replace(/[^A-Z2-7]/g, "");
  let bits = 0;
  let value = 0;
  const out = [];
  for (const ch of clean) {
    const idx = BASE32.indexOf(ch);
    if (idx === -1) continue;
    value = value << 5 | idx;
    bits += 5;
    if (bits >= 8) {
      out.push(value >>> bits - 8 & 255);
      bits -= 8;
    }
  }
  return Buffer.from(out);
}
function hotp(secret, counter) {
  const buf = Buffer.alloc(8);
  buf.writeUInt32BE(Math.floor(counter / 4294967296), 0);
  buf.writeUInt32BE(counter >>> 0, 4);
  const digest = crypto2.createHmac("sha1", secret).update(buf).digest();
  const offset = digest[digest.length - 1] & 15;
  const binary = (digest[offset] & 127) << 24 | (digest[offset + 1] & 255) << 16 | (digest[offset + 2] & 255) << 8 | digest[offset + 3] & 255;
  return String(binary % 10 ** DIGITS).padStart(DIGITS, "0");
}
function verifyTotp(secretBase32, code, at = Date.now(), window = 1) {
  const cleaned = code.replace(/\D/g, "");
  if (cleaned.length !== DIGITS) return false;
  const secret = base32Decode(secretBase32);
  if (secret.length === 0) return false;
  const counter = Math.floor(at / 1e3 / PERIOD_SECONDS);
  for (let drift = -window; drift <= window; drift++) {
    const expected = hotp(secret, counter + drift);
    if (crypto2.timingSafeEqual(Buffer.from(expected), Buffer.from(cleaned))) return true;
  }
  return false;
}
function totpUri(secretBase32, account, issuer) {
  const label = encodeURIComponent(`${issuer}:${account}`);
  const params = new URLSearchParams({
    secret: secretBase32,
    issuer,
    algorithm: "SHA1",
    digits: String(DIGITS),
    period: String(PERIOD_SECONDS)
  });
  return `otpauth://totp/${label}?${params.toString()}`;
}

// apps/server/src/services/qr.ts
import QRCode from "qrcode";
function tableUrl(publicUrl, slug, code) {
  const base = publicUrl.replace(/\/+$/, "");
  return `${base}/${encodeURIComponent(slug)}/t/${encodeURIComponent(code)}`;
}
async function tableQrSvg(url) {
  return QRCode.toString(url, {
    type: "svg",
    errorCorrectionLevel: "H",
    margin: 1,
    width: 512
  });
}
async function printableSheet(input) {
  const cards = await Promise.all(
    input.tables.map(async (t) => {
      const url = tableUrl(input.publicUrl, input.slug, t.code);
      const svg = await tableQrSvg(url);
      return `<article class="card">
        <div class="qr">${svg}</div>
        <div class="code">${escapeHtml2(t.code)}</div>
        <div class="label">${escapeHtml2(t.label ?? input.restaurantName)}</div>
        <div class="url">${escapeHtml2(url.replace(/^https?:\/\//, ""))}</div>
      </article>`;
    })
  );
  return `<!doctype html>
<meta charset="utf-8">
<title>${escapeHtml2(input.restaurantName)} \u2014 table codes</title>
<style>
  :root { --ink:#1c1b19; --muted:#6b665e; --line:#e2ded6; }
  * { box-sizing: border-box; }
  body { font-family: ui-sans-serif, system-ui, sans-serif; color: var(--ink);
         margin: 0; padding: 12mm; background:#fff; }
  h1 { font-size: 1rem; font-weight: 600; margin: 0 0 8mm; color: var(--muted); }
  .grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8mm; }
  .card { border: 1px solid var(--line); padding: 8mm; text-align: center;
          break-inside: avoid; }
  .qr svg { width: 100%; height: auto; max-width: 60mm; }
  .code { font-size: 2.5rem; font-weight: 700; letter-spacing: -.02em; margin-top: 4mm; }
  .label { font-size: .95rem; color: var(--muted); }
  .url { font-size: .7rem; color: var(--muted); margin-top: 3mm; word-break: break-all; }
  @media print {
    body { padding: 0; }
    h1 { display: none; }
    .grid { gap: 0; }
    .card { border: 1px dashed var(--line); }
  }
</style>
<h1>${escapeHtml2(input.restaurantName)} \u2014 print, cut, and stick one on each table</h1>
<div class="grid">${cards.join("")}</div>`;
}
function escapeHtml2(s) {
  return s.replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]
  );
}

// apps/server/src/services/reporting.ts
var BUSINESS_DAY_CUTOFF_HOUR = 4;
function businessDay(column, tzParam) {
  return `date_trunc('day', timezone(${tzParam}, ${column}) - interval '${BUSINESS_DAY_CUTOFF_HOUR} hours')::date`;
}
async function dailyTakings(t, days = 30) {
  const rows = await t.q(
    `WITH paid AS (
       SELECT s.id,
              ${businessDay("coalesce(s.closed_at, s.created_at)", "$2")} AS day,
              s.tip_cents,
              s.service_cents,
              s.bill_total_cents,
              s.short_cents,
              coalesce((
                SELECT sum(p.received_cents)
                  FROM payments p
                 WHERE p.session_id = s.id AND p.status = 'paid' AND p.voided_at IS NULL
              ), 0) AS taken,
              (
                SELECT count(DISTINCT coalesce(p.on_behalf_of, p.participant_id))
                  FROM payments p
                 WHERE p.session_id = s.id AND p.status = 'paid' AND p.voided_at IS NULL
              ) AS payers
         FROM sessions s
        WHERE s.status IN ('paid', 'short', 'closed')
          AND coalesce(s.closed_at, s.created_at) > now() - ($1 || ' days')::interval
     )
     -- Formatted in SQL, not in JS: a date column comes back from pg as a
     -- Date at local midnight, and turning that into an ISO day in a
     -- positive-offset timezone lands on the day before.
     SELECT to_char(day, 'YYYY-MM-DD')          AS day,
            count(*)::int                       AS sessions,
            coalesce(sum(payers), 0)::int       AS diners_paid,
            coalesce(sum(taken), 0)             AS takings,
            -- Only from tables that actually paid: a tip on an abandoned table
            -- was never collected and must not appear in what staff are owed.
            coalesce(sum(tip_cents) FILTER (WHERE taken > 0), 0) AS tips,
            coalesce(sum(service_cents) FILTER (WHERE taken > 0), 0) AS service,
            coalesce(sum(short_cents), 0)       AS shortfall
       FROM paid
      GROUP BY day
      ORDER BY day DESC`,
    [String(days), t.restaurant.timezone]
  );
  return rows.map((r) => ({
    day: String(r.day),
    sessions: Number(r.sessions),
    dinersPaid: Number(r.diners_paid),
    takingsCents: Number(r.takings),
    tipsCents: Number(r.tips),
    serviceCents: Number(r.service),
    shortfallCents: Number(r.shortfall)
  }));
}
async function tipsByTable(t, days = 30) {
  const rows = await t.q(
    `SELECT tb.code AS table_code,
            count(s.id)::int                                   AS sessions,
            coalesce(sum(s.bill_total_cents - s.tip_cents), 0)  AS food,
            coalesce(sum(s.tip_cents), 0)                       AS tips
       FROM sessions s
       JOIN tables tb ON tb.id = s.table_id
      WHERE s.status IN ('paid', 'short', 'closed')
        AND s.bill_total_cents IS NOT NULL
        AND ${businessDay("coalesce(s.closed_at, s.created_at)", "$2")}
            > timezone($2, now())::date - $1::int
      GROUP BY tb.code
      ORDER BY tips DESC`,
    [days, t.restaurant.timezone]
  );
  return rows.map((r) => {
    const food = Number(r.food);
    const tips = Number(r.tips);
    return {
      tableCode: String(r.table_code),
      sessions: Number(r.sessions),
      takingsCents: food + tips,
      tipsCents: tips,
      // Against the food, not the total: a tip is not tipped on itself.
      averageTipPercent: food > 0 ? Math.round(tips / food * 1e3) / 10 : 0
    };
  });
}
async function settlement(t, days = 30) {
  const row = await t.q.one(
    `SELECT
       coalesce(sum(p.received_cents) FILTER (WHERE p.status = 'paid' AND p.voided_at IS NULL), 0)      AS takings,
       coalesce(sum(p.received_cents) FILTER (WHERE p.status = 'underpaid'), 0)                          AS underpaid,
       coalesce(sum(p.received_cents) FILTER (WHERE p.voided_at IS NOT NULL), 0)                         AS stale,
       coalesce(sum(p.expected_cents) FILTER (WHERE p.status = 'pending' AND p.voided_at IS NULL), 0)    AS outstanding
     FROM payments p
     WHERE p.created_at > now() - ($1 || ' days')::interval`,
    [String(days)]
  );
  const totals = await t.q.one(
    `SELECT count(*)::int AS sessions,
            coalesce(sum(tip_cents) FILTER (WHERE status IN ('paid','short')), 0)     AS tips,
            coalesce(sum(service_cents) FILTER (WHERE status IN ('paid','short')), 0) AS service,
            coalesce(sum(short_cents), 0)                                              AS short_closed
       FROM sessions
      WHERE created_at > now() - ($1 || ' days')::interval`,
    [String(days)]
  );
  const providers = await t.q(
    `SELECT provider,
            count(*)::int AS count,
            coalesce(sum(received_cents), 0) AS takings
       FROM payments
      WHERE status = 'paid' AND voided_at IS NULL
        AND created_at > now() - ($1 || ' days')::interval
      GROUP BY provider
      ORDER BY takings DESC`,
    [String(days)]
  );
  return {
    takingsCents: Number(row?.takings ?? 0),
    tipsCents: Number(totals?.tips ?? 0),
    serviceCents: Number(totals?.service ?? 0),
    underpaidCents: Number(row?.underpaid ?? 0),
    staleCents: Number(row?.stale ?? 0),
    outstandingCents: Number(row?.outstanding ?? 0),
    shortClosedCents: Number(totals?.short_closed ?? 0),
    sessions: Number(totals?.sessions ?? 0),
    byProvider: providers.map((p) => ({
      provider: String(p.provider),
      count: Number(p.count),
      takingsCents: Number(p.takings)
    }))
  };
}

// apps/server/src/db/unscoped.ts
async function platformTotals(db) {
  const row = await db.withPlatform(
    (q) => q.one(
      `SELECT (SELECT count(*) FROM restaurants WHERE archived_at IS NULL)          AS restaurants,
              (SELECT count(*) FROM sessions WHERE created_at >= date_trunc('day', now())) AS sessions_today,
              (SELECT coalesce(sum(cost_micros), 0) FROM ai_usage)                  AS ai_cost_micros`
    )
  );
  return {
    restaurants: Number(row?.restaurants ?? 0),
    sessionsToday: Number(row?.sessions_today ?? 0),
    aiCostMicros: Number(row?.ai_cost_micros ?? 0)
  };
}
async function restaurantForSessionToken(db, tokenHash) {
  const row = await db.withPlatform(
    (q) => q.one(
      `SELECT restaurant_id FROM restaurant_sessions
        WHERE token_hash = $1 AND expires_at > now() AND revoked_at IS NULL`,
      [tokenHash]
    )
  );
  return row?.restaurant_id ?? null;
}

// apps/server/src/routes/staff.ts
var COOKIE = "sharyt_staff";
var SESSION_HOURS = 12;
function staffRoutes(ctx) {
  const router = Router2();
  const cookieOptions = (req) => ({
    httpOnly: true,
    sameSite: "lax",
    // Pinned from configuration, never from a client-supplied
    // X-Forwarded-Proto: a request that omits the header would otherwise get a
    // cookie without the Secure flag.
    secure: ctx.trustProxy ? true : req.protocol === "https",
    path: "/",
    maxAge: SESSION_HOURS * 36e5
  });
  function checkOrigin(req) {
    if (req.method === "GET" || req.method === "HEAD") return;
    const fetchSite = req.get("sec-fetch-site");
    if (fetchSite === "same-origin" || fetchSite === "none") return;
    if (fetchSite) throw ApiError.forbidden("Cross-site request refused.");
    const origin = req.get("origin");
    if (!origin) throw ApiError.forbidden("Missing Origin. Refusing a cookie-authenticated write.");
    const host = req.get("host");
    try {
      if (new URL(origin).host !== host) throw new Error("mismatch");
    } catch {
      throw ApiError.forbidden("Cross-site request refused.");
    }
  }
  async function authenticate(req) {
    const token = req.cookies?.[COOKIE];
    if (!token) throw ApiError.unauthorized("Sign in.");
    const restaurantId = await restaurantForSessionToken(ctx.db, hashToken(token));
    if (!restaurantId) throw ApiError.unauthorized("Sign in.");
    const resolved = await ctx.tenant(restaurantId, (t) => t.staff.resolveSession(token));
    if (!resolved) throw ApiError.unauthorized("Sign in.");
    req.staff = {
      userId: resolved.user.id,
      restaurantId,
      role: resolved.user.role,
      sessionId: resolved.session.id
    };
  }
  function requireStaff(permission) {
    return async (req, _res, next) => {
      try {
        checkOrigin(req);
        await authenticate(req);
        if (permission && !can(req.staff.role, permission)) {
          throw ApiError.forbidden("Your role does not allow that.");
        }
        next();
      } catch (err) {
        next(err);
      }
    };
  }
  const asTenant = (req, fn) => ctx.tenant(req.staff.restaurantId, fn);
  router.post("/signup", async (req, res) => {
    const ip = req.ip ?? "unknown";
    if (!ctx.limiter.hit(platformKey("signup", ip), 5, 36e5).allowed) {
      throw ApiError.tooMany("Too many sign-ups from here. Try again later.");
    }
    const body = signupSchema.parse(req.body);
    try {
      const restaurant = await ctx.registry.create({
        slug: body.slug,
        name: body.name,
        currency: body.currency
      });
      const owner = await ctx.tenant(
        restaurant.id,
        (t) => t.staff.create({ email: body.email, password: body.password, role: "owner" })
      );
      const delivery = await sendVerification(ctx, {
        restaurantId: restaurant.id,
        userId: owner.id,
        email: body.email,
        restaurantName: restaurant.name,
        slug: restaurant.slug
      });
      res.status(201).json({
        ok: true,
        slug: restaurant.slug,
        ownerId: owner.id,
        emailSent: delivery.sent,
        // On a fresh install with no mail server, this is how anyone gets in.
        ...delivery.fallbackNotice ? { notice: delivery.fallbackNotice } : {}
      });
    } catch (err) {
      if (err instanceof SlugRejected) throw ApiError.badRequest(err.message);
      if (String(err.message).includes("restaurants_slug_key")) {
        throw ApiError.conflict("That name is taken.");
      }
      throw err;
    }
  });
  router.post("/verify", async (req, res) => {
    const token = String(req.body.token ?? "");
    if (!await verifyEmail(ctx, token)) {
      throw ApiError.badRequest("That link has expired or has already been used.");
    }
    res.json({ ok: true });
  });
  router.post("/reset", async (req, res) => {
    const body = requestResetSchema.parse(req.body);
    if (!ctx.limiter.hit(platformKey("reset", req.ip ?? "unknown"), 5, 9e5).allowed) {
      throw ApiError.tooMany("Too many reset requests. Wait a few minutes.");
    }
    const delivery = await requestPasswordReset(ctx, body);
    res.json({
      ok: true,
      message: "If that address has an account here, a reset link is on its way.",
      ...delivery.fallbackNotice ? { notice: delivery.fallbackNotice } : {}
    });
  });
  router.post("/reset/complete", async (req, res) => {
    const body = completeResetSchema.parse(req.body);
    if (!await completePasswordReset(ctx, body.token, body.password)) {
      throw ApiError.badRequest("That link has expired or has already been used.");
    }
    res.clearCookie(COOKIE, { path: "/" });
    res.json({ ok: true });
  });
  router.post("/password", requireStaff(), async (req, res) => {
    const body = changePasswordSchema.parse(req.body);
    const ok = await asTenant(req, async (t) => {
      const row = await t.q.one(
        "SELECT password_hash, email FROM restaurant_users WHERE id = $1",
        [req.staff.userId]
      );
      if (!row || !verifyPassword(body.currentPassword, row.password_hash)) return false;
      await changePassword(t, req.staff.userId, body.newPassword);
      return true;
    });
    if (!ok) throw ApiError.badRequest("That current password is not right.");
    res.json({ ok: true });
  });
  router.post("/mfa/start", requireStaff(), async (req, res) => {
    const secret = generateTotpSecret();
    const out = await asTenant(req, async (t) => {
      const user = await t.staff.byId(req.staff.userId);
      return {
        secret,
        // Shown as a QR and as text, because plenty of people enrol on the same
        // phone they are reading this on and cannot scan their own screen.
        uri: totpUri(secret, user?.email ?? "staff", t.restaurant.name)
      };
    });
    res.json({ ok: true, ...out });
  });
  router.post("/mfa/enable", requireStaff(), async (req, res) => {
    const body = enableMfaSchema.parse(req.body);
    if (!verifyTotp(body.secret, body.code)) {
      throw ApiError.badRequest("That code is not right. Check your authenticator and try the next one.");
    }
    await asTenant(req, async (t) => {
      await t.staff.setTotpSecret(req.staff.userId, encryptSecret(body.secret, ctx.appKey));
      await t.q("UPDATE restaurant_users SET logins_without_mfa = 0 WHERE id = $1", [
        req.staff.userId
      ]);
      await t.telemetry.audit({
        actorType: "staff",
        actorId: req.staff.userId,
        action: "staff.mfa_enabled",
        ip: req.ip ?? null
      });
    });
    res.json({ ok: true });
  });
  router.get("/gate", requireStaff(), async (req, res) => {
    const out = await asTenant(req, async (t) => {
      const user = await t.staff.byId(req.staff.userId);
      const row = await t.q.one(
        "SELECT logins_without_mfa, must_change_password FROM restaurant_users WHERE id = $1",
        [req.staff.userId]
      );
      const count = row?.logins_without_mfa ?? 0;
      const hasMfa = user?.hasTotp ?? false;
      return {
        mustChangePassword: row?.must_change_password ?? false,
        mfaSuggested: !hasMfa && count <= MFA_GRACE_LOGINS,
        mfaRequired: !hasMfa && count > MFA_GRACE_LOGINS,
        loginsWithoutMfa: count,
        graceRemaining: hasMfa ? 0 : Math.max(0, MFA_GRACE_LOGINS - count + 1)
      };
    });
    res.json({ ok: true, gate: out });
  });
  router.post("/login", async (req, res) => {
    checkOrigin(req);
    const body = staffLoginSchema.parse(req.body);
    const ip = req.ip ?? "unknown";
    const restaurant = await ctx.registry.bySlug(body.slug);
    const byIp = ctx.limiter.hit(platformKey("login-ip", ip), LIMITS.loginIp.limit, LIMITS.loginIp.windowMs);
    const byAccount = ctx.limiter.hit(
      tenantKey(restaurant?.id ?? "unknown", "login", body.email.toLowerCase()),
      LIMITS.loginAccount.limit,
      LIMITS.loginAccount.windowMs
    );
    if (!byIp.allowed || !byAccount.allowed) {
      throw ApiError.tooMany("Too many sign-in attempts. Wait a few minutes.");
    }
    if (!restaurant) throw ApiError.unauthorized("That email and password do not match.");
    const result = await ctx.tenant(restaurant.id, async (t) => {
      const user = await t.staff.verifyCredentials(body.email, body.password);
      if (!user) return null;
      if (!await isVerified(t, user.id)) return { unverified: true };
      if (user.hasTotp) {
        if (!body.totp) return { totpRequired: true };
        const envelope = await t.staff.totpSecretEnvelope(user.id);
        let secret = "";
        try {
          secret = envelope ? decryptSecret(envelope, ctx.appKey) : "";
        } catch {
          secret = "";
        }
        if (!secret || !verifyTotp(secret, body.totp)) return { totpRequired: true };
      }
      await t.staff.recordLogin(user.id);
      const gate = await evaluateLogin(t, user.id);
      const { token } = await t.staff.createSession({
        userId: user.id,
        ttlHours: SESSION_HOURS,
        ip,
        userAgent: req.get("user-agent") ?? null
      });
      return { user, token, gate };
    });
    if (!result) throw ApiError.unauthorized("That email and password do not match.");
    if ("unverified" in result) {
      throw new ApiError(403, "Confirm your email address first. Check your inbox.", {
        unverified: true
      });
    }
    if ("totpRequired" in result) {
      throw new ApiError(401, "Enter the code from your authenticator app.", { totpRequired: true });
    }
    ctx.limiter.reset(tenantKey(restaurant.id, "login", body.email.toLowerCase()));
    res.cookie(COOKIE, result.token, cookieOptions(req));
    res.json({
      ok: true,
      user: { id: result.user.id, email: result.user.email, role: result.user.role },
      restaurant: { slug: restaurant.slug, name: restaurant.name },
      // What this account has to deal with before anything else.
      gate: result.gate
    });
  });
  router.post("/logout", requireStaff(), async (req, res) => {
    const token = req.cookies[COOKIE];
    await asTenant(req, (t) => t.staff.revokeByToken(token));
    res.clearCookie(COOKIE, { path: "/" });
    res.json({ ok: true });
  });
  router.get("/me", requireStaff(), async (req, res) => {
    const out = await asTenant(req, async (t) => ({
      user: await t.staff.byId(req.staff.userId),
      restaurant: {
        slug: t.restaurant.slug,
        name: t.restaurant.name,
        currency: t.restaurant.currency,
        mockMode: t.restaurant.mockMode,
        liveEnabledAt: t.restaurant.liveEnabledAt
      }
    }));
    res.json({ ok: true, ...out });
  });
  router.post("/staff", requireStaff("staff.manage"), async (req, res) => {
    const body = createStaffSchema.parse(req.body);
    try {
      const user = await asTenant(req, async (t) => {
        const created = await t.staff.create(body);
        await t.q(
          `UPDATE restaurant_users
              SET must_change_password = TRUE, email_verified_at = now()
            WHERE id = $1`,
          [created.id]
        );
        return created;
      });
      res.status(201).json({ ok: true, user });
    } catch (err) {
      if (err instanceof EmailTakenError) throw ApiError.conflict(err.message);
      throw err;
    }
  });
  router.get("/tables", requireStaff(), async (req, res) => {
    const board = await asTenant(req, async (t) => {
      const tables = await t.tables.list();
      const board2 = [];
      for (const table of tables) {
        const session = await t.sessions.liveForTable(table.id);
        if (!session) {
          board2.push({ ...table, session: null });
          continue;
        }
        const diners = await t.sessions.participants(session.id);
        const money = await t.payments.reconcile(session.id);
        board2.push({
          ...table,
          session: {
            id: session.id,
            status: session.status,
            headcount: session.headcount,
            billTotalCents: session.billTotalCents,
            paidCents: money.paidCents,
            diners: diners.length
          }
        });
      }
      return board2;
    });
    res.json({ ok: true, tables: board });
  });
  router.post("/tables", requireStaff("table.manage"), async (req, res) => {
    const body = createTableSchema.parse(req.body);
    try {
      const table = await asTenant(req, (t) => t.tables.create(body));
      res.status(201).json({ ok: true, table });
    } catch (err) {
      if (err instanceof CodeTakenError) throw ApiError.conflict(err.message);
      if (err instanceof CodeRetiredError) throw ApiError.conflict(err.message);
      throw err;
    }
  });
  router.patch("/tables/:id", requireStaff("table.manage"), async (req, res) => {
    const body = updateTableSchema.parse(req.body);
    try {
      const table = await asTenant(req, (t) => t.tables.update(String(req.params.id), body));
      if (!table) throw ApiError.notFound("No such table.");
      res.json({ ok: true, table, reprintRequired: body.code !== void 0 });
    } catch (err) {
      if (err instanceof CodeTakenError) throw ApiError.conflict(err.message);
      if (err instanceof CodeRetiredError) throw ApiError.conflict(err.message);
      throw err;
    }
  });
  router.get("/payments", requireStaff("settings.manage"), async (req, res) => {
    const out = await asTenant(req, async (t) => ({
      current: await pspSummary(ctx, t),
      available: describeProviders(t.restaurant.currency),
      currency: t.restaurant.currency,
      webhookUrl: `${publicUrl(req)}/api/webhooks/psp/${t.restaurant.webhookToken}`
    }));
    res.json({ ok: true, ...out });
  });
  router.put("/payments/provider", requireStaff("settings.manage"), async (req, res) => {
    const providerId = String(req.body.providerId ?? "");
    try {
      await asTenant(req, async (t) => {
        await setProvider(t, providerId);
        await t.telemetry.audit({
          actorType: "staff",
          actorId: req.staff.userId,
          action: "psp.provider_changed",
          meta: { providerId },
          ip: req.ip ?? null
        });
      });
    } catch (err) {
      if (err instanceof ProviderConfigError) throw ApiError.badRequest(err.message);
      throw err;
    }
    res.json({ ok: true });
  });
  router.put("/payments/credentials", requireStaff("settings.manage"), async (req, res) => {
    const body = req.body;
    if (!body.providerId) throw ApiError.badRequest("Which provider?");
    await asTenant(req, async (t) => {
      await saveCredentials(ctx, t, body.providerId, body.values ?? {});
      await t.telemetry.audit({
        actorType: "staff",
        actorId: req.staff.userId,
        action: "psp.credentials_updated",
        meta: { providerId: body.providerId, fields: Object.keys(body.values ?? {}) },
        ip: req.ip ?? null
      });
    });
    res.json({ ok: true });
  });
  router.delete(
    "/payments/credentials/:providerId/:field",
    requireStaff("settings.manage"),
    async (req, res) => {
      await asTenant(req, (t) => clearCredential(t, String(req.params.providerId), String(req.params.field)));
      res.json({ ok: true });
    }
  );
  router.post("/payments/go-live", requireStaff("settings.manage"), async (req, res) => {
    const result = await asTenant(req, async (t) => {
      const origin = publicUrl(req);
      const out = await attemptGoLive(ctx, t, {
        returnUrl: `${origin}/${t.restaurant.slug}`,
        cancelUrl: `${origin}/${t.restaurant.slug}`,
        webhookUrl: `${origin}/api/webhooks/psp/${t.restaurant.webhookToken}`
      });
      await t.telemetry.audit({
        actorType: "staff",
        actorId: req.staff.userId,
        action: out.ok ? "psp.went_live" : "psp.go_live_refused",
        meta: { reason: out.reason ?? null },
        ip: req.ip ?? null
      });
      return out;
    });
    if (!result.ok) throw ApiError.badRequest(result.reason ?? "Could not go live.");
    res.json({ ok: true });
  });
  router.get("/receipts/:reference", requireStaff("receipt.print"), async (req, res) => {
    const html = await asTenant(req, async (t) => {
      const payment = await t.payments.byReference(String(req.params.reference));
      if (!payment) return null;
      const receipt = await buildReceipt(t, payment);
      return receipt ? receiptHtml(receipt) : null;
    });
    if (!html) throw ApiError.notFound("No such payment.");
    res.type("html").send(html);
  });
  router.get("/reports", requireStaff("report.read"), async (req, res) => {
    const days = Math.min(365, Math.max(1, Number(req.query.days) || 30));
    const out = await asTenant(req, async (t) => ({
      days,
      daily: await dailyTakings(t, days),
      tables: await tipsByTable(t, days),
      settlement: await settlement(t, days),
      currency: t.restaurant.currency
    }));
    res.json({ ok: true, ...out });
  });
  router.get("/cashier", requireStaff(), async (req, res) => {
    const out = await asTenant(req, async (t) => {
      const rows = await t.q(
        `SELECT p.reference, p.status, p.expected_cents, p.received_cents, p.voided_at,
                p.created_at, p.paid_at, p.provider,
                tb.code AS table_code,
                coalesce(sp.name, 'Guest ' || coalesce(sp.seat_no::text, '')) AS payer
           FROM payments p
           JOIN sessions s ON s.id = p.session_id
           JOIN tables tb ON tb.id = s.table_id
           LEFT JOIN session_participants sp
                  ON sp.id = coalesce(p.on_behalf_of, p.participant_id)
          WHERE p.created_at > now() - interval '12 hours'
          ORDER BY p.created_at DESC
          LIMIT 100`
      );
      return rows.map((r) => ({
        reference: r.reference,
        tableCode: r.table_code,
        payer: String(r.payer).trim(),
        status: r.status,
        // Flagged separately: money against a link the bill had already
        // invalidated is real, and it needs a person rather than a diner.
        stale: r.voided_at !== null && r.received_cents !== null,
        expectedCents: Number(r.expected_cents),
        receivedCents: r.received_cents === null ? null : Number(r.received_cents),
        provider: r.provider,
        at: r.paid_at ?? r.created_at
      }));
    });
    res.json({ ok: true, payments: out });
  });
  router.post("/receipts/:reference/email", requireStaff("receipt.print"), async (req, res) => {
    const to = String(req.body.email ?? "").trim();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(to)) throw ApiError.badRequest("That is not an email address.");
    const message = await asTenant(req, async (t) => {
      const payment = await t.payments.byReference(String(req.params.reference));
      if (!payment) return null;
      const receipt = await buildReceipt(t, payment);
      if (!receipt) return null;
      await t.telemetry.audit({
        actorType: "staff",
        actorId: req.staff.userId,
        action: "receipt.emailed",
        targetId: payment.reference,
        ip: req.ip ?? null
      });
      return {
        to,
        subject: `Your receipt from ${receipt.restaurantName}`,
        text: receiptText(receipt)
      };
    });
    if (!message) throw ApiError.notFound("No such payment.");
    const delivery = await ctx.mail.send(message);
    res.json({ ok: true, sent: delivery.sent, ...delivery.fallbackNotice ? { notice: delivery.fallbackNotice } : {} });
  });
  router.get("/settings", requireStaff(), async (req, res) => {
    res.json({ ok: true, settings: await asTenant(req, (t) => describeSettings(t)) });
  });
  router.put("/settings", requireStaff("settings.manage"), async (req, res) => {
    const values = req.body.values ?? {};
    try {
      await asTenant(req, async (t) => {
        await writeSettings(t, values);
        await t.telemetry.audit({
          actorType: "staff",
          actorId: req.staff.userId,
          action: "settings.updated",
          meta: { keys: Object.keys(values) },
          ip: req.ip ?? null
        });
      });
    } catch (err) {
      if (err instanceof UnknownSetting) throw ApiError.badRequest(err.message);
      throw err;
    }
    res.json({ ok: true });
  });
  router.get("/ai", requireStaff("settings.manage"), async (req, res) => {
    res.json({ ok: true, ...await asTenant(req, (t) => visionSummary(t)) });
  });
  router.put("/ai", requireStaff("settings.manage"), async (req, res) => {
    const body = req.body;
    await asTenant(req, async (t) => {
      await saveVisionSettings(ctx, t, body);
      await t.telemetry.audit({
        actorType: "staff",
        actorId: req.staff.userId,
        action: "ai.settings_updated",
        // Never the key itself, only that it changed.
        meta: { fields: Object.keys(body), keyChanged: Boolean(body.apiKey?.trim()) },
        ip: req.ip ?? null
      });
    });
    res.json({ ok: true });
  });
  router.get("/tables/:id/qr.svg", requireStaff(), async (req, res) => {
    const out = await asTenant(req, async (t) => {
      const table = await t.tables.byId(String(req.params.id));
      if (!table) return null;
      return { url: tableUrl(publicUrl(req), t.restaurant.slug, table.code) };
    });
    if (!out) throw ApiError.notFound("No such table.");
    res.type("image/svg+xml").send(await tableQrSvg(out.url));
  });
  router.get("/tables/print", requireStaff(), async (req, res) => {
    const out = await asTenant(req, async (t) => ({
      restaurantName: t.restaurant.name,
      slug: t.restaurant.slug,
      tables: (await t.tables.list()).map((x) => ({ code: x.code, label: x.label }))
    }));
    res.type("html").send(await printableSheet({ ...out, publicUrl: publicUrl(req) }));
  });
  router.post("/sessions", requireStaff("session.open"), async (req, res) => {
    const body = openSessionSchema.parse(req.body);
    try {
      const session = await asTenant(
        req,
        (t) => t.sessions.open(body.tableId, {
          openedBy: req.staff.userId,
          currency: t.restaurant.currency
        })
      );
      res.status(201).json({ ok: true, session });
    } catch (err) {
      if (err instanceof TableBusyError) throw ApiError.conflict(err.message);
      throw err;
    }
  });
  router.get("/sessions/:id", requireStaff(), async (req, res) => {
    const id = String(req.params.id);
    const detail = await asTenant(req, async (t) => {
      const full = await t.sessions.full(id);
      if (!full) return null;
      const payments = await t.payments.forSession(id);
      const recon = await reconcile(t, id);
      const table = await t.tables.byId(full.session.tableId);
      return { session: full.session, table, participants: full.participants, payments, recon };
    });
    if (!detail) throw ApiError.notFound("No such table session.");
    res.json({ ok: true, ...detail });
  });
  router.post("/sessions/:id/close", requireStaff("session.close"), async (req, res) => {
    const id = String(req.params.id);
    const result = await asTenant(req, async (t) => {
      const recon = await reconcile(t, id);
      if (!recon.canClosePaid && recon.outstandingCents > 0) {
        return { blocked: true, recon };
      }
      const session = await t.sessions.setStatus(id, "paid");
      ctx.events.broadcast(channelFor(t.restaurantId, id), "closed", {});
      return { blocked: false, session, recon };
    });
    if (result.blocked) {
      throw ApiError.conflict(
        `This table is still ${result.recon.outstandingCents} cents short. Close it short if that is deliberate.`
      );
    }
    res.json({ ok: true, ...result });
  });
  router.post(
    "/sessions/:id/close-short",
    requireStaff("session.close_short"),
    async (req, res) => {
      const id = String(req.params.id);
      const body = closeShortSchema.parse(req.body);
      const out = await asTenant(req, async (t) => {
        const recon = await reconcile(t, id);
        const shortfall = Math.max(
          0,
          recon.billTotalCents - recon.paidCents - recon.underpaidCents - recon.staleCents
        );
        const session = await t.sessions.closeShort(id, shortfall, body.reason);
        await t.telemetry.audit({
          actorType: "staff",
          actorId: req.staff.userId,
          action: "session.close_short",
          sessionId: id,
          meta: { shortfall, reason: body.reason, recon },
          ip: req.ip ?? null
        });
        ctx.events.broadcast(channelFor(t.restaurantId, id), "closed", {});
        return { session, shortfall, recon };
      });
      res.json({ ok: true, ...out });
    }
  );
  return router;
  function publicUrl(req) {
    return ctx.publicUrl ?? `${req.protocol}://${req.get("host") ?? "localhost:3000"}`;
  }
}

// apps/server/src/routes/mock.ts
import { Router as Router3 } from "express";
var REFERENCE = /^[a-z0-9_.-]{1,120}$/;
function mockRoutes(ctx) {
  const router = Router3({ mergeParams: true });
  async function tenantOr404(req) {
    const slug = String(req.params.slug ?? "");
    const restaurant = await ctx.registry.bySlug(slug);
    if (!restaurant) throw ApiError.notFound("No such restaurant.");
    if (!restaurant.mockMode) throw ApiError.notFound("Not found.");
    return restaurant;
  }
  router.get("/checkout", async (req, res) => {
    const restaurant = await tenantOr404(req);
    const reference = String(req.query.reference ?? "");
    if (!REFERENCE.test(reference)) throw ApiError.notFound("Not found.");
    const payment = await ctx.tenant(restaurant.id, (t) => t.payments.byReference(reference));
    if (!payment) throw ApiError.notFound("No such checkout.");
    const amount = formatMoney(payment.expectedCents, payment.currency);
    res.type("html").send(`<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Simulated checkout</title>
<style>
  body { font-family: ui-sans-serif, system-ui, sans-serif; background:#f7f6f3; color:#1c1b19;
         margin:0; display:grid; place-items:center; min-height:100vh; padding:1rem; }
  .card { background:#fff; border:1px solid #e2ded6; padding:2rem; max-width:26rem; width:100%; }
  h1 { font-size:1.1rem; margin:0 0 .25rem; letter-spacing:-.01em; }
  .amount { font-size:2.25rem; font-weight:600; margin:1rem 0; font-variant-numeric:tabular-nums; }
  .note { background:#fff4e8; border:1px solid #f0d9bd; padding:.75rem; font-size:.85rem; margin:1rem 0; }
  button { width:100%; padding:.9rem; border:0; background:#a73a00; color:#fff; font-size:1rem;
           cursor:pointer; }
  button.ghost { background:transparent; color:#6b665e; border:1px solid #e2ded6; margin-top:.5rem; }
</style>
<div class="card">
  <h1>${escapeHtml3(restaurant.name)}</h1>
  <div class="amount">${escapeHtml3(amount)}</div>
  <div class="note"><strong>This is not a real payment.</strong> This restaurant has not
    connected a live payment account yet, so nothing will be charged and no money will move.</div>
  <form method="post" action="/${encodeURIComponent(restaurant.slug)}/mock/pay">
    <input type="hidden" name="reference" value="${escapeHtml3(reference)}">
    <button type="submit">Simulate a successful payment</button>
  </form>
  <form method="post" action="/${encodeURIComponent(restaurant.slug)}/mock/fail">
    <input type="hidden" name="reference" value="${escapeHtml3(reference)}">
    <button type="submit" class="ghost">Simulate a failure</button>
  </form>
</div>`);
  });
  router.post("/pay", async (req, res) => {
    const restaurant = await tenantOr404(req);
    const reference = String(req.body?.reference ?? "");
    if (!REFERENCE.test(reference)) throw ApiError.notFound("Not found.");
    const outcome = await ctx.tenant(restaurant.id, async (t) => {
      const payment = await t.payments.byReference(reference);
      if (!payment) return null;
      const settled = await t.payments.settle({
        reference,
        receivedCents: payment.expectedCents,
        currency: payment.currency,
        providerRef: `mock_${Date.now()}`
      });
      return { sessionId: payment.sessionId, settled };
    });
    if (!outcome) throw ApiError.notFound("No such checkout.");
    await ctx.tenant(restaurant.id, (t) => settleAndBroadcast(ctx, t, outcome.sessionId));
    res.redirect(303, `/${encodeURIComponent(restaurant.slug)}/paid?ref=${encodeURIComponent(reference)}`);
  });
  router.post("/fail", async (req, res) => {
    const restaurant = await tenantOr404(req);
    const reference = String(req.body?.reference ?? "");
    if (!REFERENCE.test(reference)) throw ApiError.notFound("Not found.");
    await ctx.tenant(restaurant.id, (t) => t.payments.markFailed(reference));
    res.redirect(303, `/${encodeURIComponent(restaurant.slug)}/paid?ref=${encodeURIComponent(reference)}&failed=1`);
  });
  return router;
}
function escapeHtml3(s) {
  return s.replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]
  );
}

// apps/server/src/routes/pay.ts
import { Router as Router4 } from "express";
function payRoutes(ctx) {
  const router = Router4({ mergeParams: true });
  router.get("/:reference", async (req, res) => {
    const slug = String(req.params.slug ?? "");
    const reference = String(req.params.reference ?? "");
    const restaurant = await ctx.registry.bySlug(slug);
    if (!restaurant) throw ApiError.notFound("No such restaurant.");
    const out = await ctx.tenant(restaurant.id, async (t) => {
      const payment = await t.payments.byReference(reference);
      if (!payment) return null;
      if (payment.status !== "pending" || payment.voidedAt !== null) {
        return { stale: true };
      }
      const session = await t.sessions.byId(payment.sessionId);
      const table = session ? await t.tables.byId(session.tableId) : null;
      const origin = ctx.publicUrl ?? `${req.protocol}://${req.get("host") ?? "localhost:3000"}`;
      const tableUrl2 = `${origin}/${restaurant.slug}/t/${encodeURIComponent(table?.code ?? "")}`;
      const psp = await resolvePsp(ctx, t, {
        returnUrl: `${tableUrl2}?paid=1`,
        cancelUrl: tableUrl2,
        webhookUrl: `${origin}/api/webhooks/psp/${restaurant.webhookToken}`
      });
      const handoff = await psp.provider.initiate(
        {
          reference,
          amountCents: payment.expectedCents,
          description: `${restaurant.name} table ${table?.code ?? ""}`.trim(),
          email: `guest+${payment.sessionId}@${restaurant.slug}.sharyt.invalid`,
          metadata: { restaurantId: restaurant.id, restaurantName: restaurant.name }
        },
        psp.config
      );
      return { stale: false, handoff, name: psp.provider.displayName, tableUrl: tableUrl2 };
    });
    if (!out) throw ApiError.notFound("No such checkout.");
    if (out.stale) {
      throw ApiError.conflict("That payment link is no longer valid. Go back to the table and try again.");
    }
    if (out.handoff.kind === "redirect") {
      res.redirect(303, out.handoff.url);
      return;
    }
    res.type("html").send(autoPost(out.handoff.url, out.handoff.fields, out.name, out.tableUrl));
  });
  return router;
}
function autoPost(action, fields, providerName, backUrl) {
  const inputs = Object.entries(fields).map(([k, v]) => `<input type="hidden" name="${esc(k)}" value="${esc(v)}">`).join("\n    ");
  return `<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Taking you to ${esc(providerName)}\u2026</title>
<style>
  body { font-family: ui-sans-serif, system-ui, sans-serif; background:#f7f6f3; color:#1c1b19;
         margin:0; display:grid; place-items:center; min-height:100vh; padding:1rem; }
  .card { background:#fff; border:1px solid #e2ded6; padding:2rem; max-width:26rem; width:100%;
          text-align:center; }
  button { width:100%; padding:.9rem; border:0; background:#a73a00; color:#fff; font-size:1rem;
           margin-top:1rem; cursor:pointer; }
  a { color:#6b665e; font-size:.85rem; }
</style>
<div class="card">
  <p>Taking you to <strong>${esc(providerName)}</strong> to pay\u2026</p>
  <form id="f" method="post" action="${esc(action)}">
    ${inputs}
    <noscript><button type="submit">Continue to ${esc(providerName)}</button></noscript>
  </form>
  <p><a href="${esc(backUrl)}">Cancel and go back to the table</a></p>
</div>
<script>document.getElementById('f').submit();</script>`;
}
function esc(s) {
  return s.replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]
  );
}

// apps/server/src/routes/webhooks.ts
import { Router as Router5 } from "express";
import express from "express";

// apps/server/src/db/telemetry.ts
import { createHash as createHash4 } from "node:crypto";
function summarisePayload(raw) {
  const hash2 = createHash4("sha256").update(raw).digest("hex").slice(0, 16);
  return `sha256:${hash2} len=${raw.length} head=${JSON.stringify(raw.slice(0, 400))}`;
}
var TelemetryRepository = class {
  constructor(q, restaurantId) {
    this.q = q;
    this.restaurantId = restaurantId;
  }
  async audit(entry) {
    await this.q(
      `INSERT INTO audit_log (id, restaurant_id, session_id, actor_type, actor_id,
                              actor_label, action, target_type, target_id, meta, ip)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [
        newId("aud"),
        this.restaurantId,
        entry.sessionId ?? null,
        entry.actorType,
        entry.actorId ?? null,
        entry.actorLabel ?? null,
        entry.action,
        entry.targetType ?? null,
        entry.targetId ?? null,
        entry.meta ? JSON.stringify(entry.meta) : null,
        entry.ip ?? null
      ]
    );
  }
  async listAudit(limit = 100) {
    return this.q(
      `SELECT id, session_id, actor_type, actor_label, action, target_type, target_id, meta, ip, created_at
         FROM audit_log ORDER BY created_at DESC LIMIT $1`,
      [limit]
    );
  }
  /**
   * Record a delivery for this tenant.
   *
   * Returns false when the event id has already been seen, which is what makes
   * a Paystack retry idempotent. The uniqueness is per tenant: event ids come
   * from each restaurant's own Paystack account, so a global index would let
   * one restaurant's ids suppress another's genuine deliveries -- by accident
   * through collision, or deliberately, since a tenant holds its own signing
   * key and can mint valid events at will.
   */
  async recordWebhook(rec) {
    const rows = await this.q(
      `INSERT INTO webhook_events (id, restaurant_id, provider, event_id, event_type,
                                   signature_valid, status, reference, payload, error, processed_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
       ON CONFLICT (restaurant_id, provider, event_id) WHERE event_id IS NOT NULL DO NOTHING
       RETURNING id`,
      [
        newId("wh"),
        this.restaurantId,
        rec.provider,
        rec.eventId,
        rec.eventType,
        rec.signatureValid,
        rec.status,
        rec.reference ?? null,
        rec.payload ?? null,
        rec.error ?? null
      ]
    );
    return rows.length > 0;
  }
  async listWebhooks(limit = 100) {
    return this.q(
      `SELECT id, provider, event_id, event_type, signature_valid, status, reference, error, received_at
         FROM webhook_events ORDER BY received_at DESC LIMIT $1`,
      [limit]
    );
  }
  async recordAiUsage(u) {
    await this.q(
      `INSERT INTO ai_usage (id, restaurant_id, session_id, operation, model, input_tokens,
                             output_tokens, cache_read_tokens, cache_write_tokens,
                             cost_micros, latency_ms, ok, error)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [
        newId("ai"),
        this.restaurantId,
        u.sessionId ?? null,
        u.operation,
        u.model,
        u.inputTokens,
        u.outputTokens,
        u.cacheReadTokens ?? 0,
        u.cacheWriteTokens ?? 0,
        u.costMicros,
        u.latencyMs,
        u.ok,
        u.error ?? null
      ]
    );
  }
  async aiSpendMicros() {
    const row = await this.q.one("SELECT coalesce(sum(cost_micros),0) AS total FROM ai_usage");
    return Number(row?.total ?? 0);
  }
};
async function recordUnroutedWebhook(db, rec) {
  await db.withPlatform(
    (q) => q(
      `INSERT INTO webhook_events (id, restaurant_id, provider, event_id, event_type,
                                   signature_valid, status, reference, payload, error, processed_at)
       VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, $9, now())`,
      [
        newId("wh"),
        rec.provider,
        rec.eventId ?? null,
        rec.eventType,
        rec.signatureValid,
        rec.status,
        rec.reference ?? null,
        rec.payload ?? null,
        rec.error ?? null
      ]
    )
  );
}

// apps/server/src/routes/webhooks.ts
function webhookRoutes(ctx) {
  const router = Router5();
  const handler = async (req, res) => {
    const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.from("");
    const token = String(req.params.webhookToken ?? "");
    if (!ctx.limiter.hit(platformKey("webhook", req.ip ?? "unknown"), 240, 6e4).allowed) {
      res.sendStatus(429);
      return;
    }
    const restaurant = await ctx.registry.byWebhookToken(token);
    if (!restaurant) {
      await recordUnroutedWebhook(ctx.db, {
        provider: "unknown",
        eventType: null,
        signatureValid: false,
        status: "unknown_token",
        payload: summarisePayload(raw.toString("utf8")),
        error: "No restaurant matches this webhook token."
      });
      res.sendStatus(401);
      return;
    }
    const origin = ctx.publicUrl ?? `${req.protocol}://${req.get("host") ?? "localhost:3000"}`;
    const outcome = await ctx.tenant(restaurant.id, async (t) => {
      let psp;
      try {
        psp = await resolvePsp(ctx, t, {
          returnUrl: `${origin}/${restaurant.slug}`,
          cancelUrl: `${origin}/${restaurant.slug}`,
          webhookUrl: `${origin}/api/webhooks/psp/${restaurant.webhookToken}`
        });
      } catch (err) {
        await t.telemetry.recordWebhook({
          provider: "unknown",
          eventId: null,
          eventType: null,
          signatureValid: false,
          status: "not_configured",
          error: err.message
        });
        return { status: 400 };
      }
      const verdict = await psp.provider.verifyWebhook(
        {
          raw,
          headers: req.headers,
          sourceIp: req.ip ?? null
        },
        psp.config
      );
      if (verdict.kind === "rejected") {
        await t.telemetry.recordWebhook({
          provider: psp.provider.id,
          eventId: null,
          eventType: null,
          signatureValid: false,
          status: "rejected",
          payload: summarisePayload(raw.toString("utf8")),
          error: verdict.reason
        });
        return { status: 401 };
      }
      if (verdict.kind === "ignored") {
        await t.telemetry.recordWebhook({
          provider: psp.provider.id,
          eventId: null,
          eventType: null,
          signatureValid: true,
          status: "ignored",
          error: verdict.reason
        });
        return { status: 200 };
      }
      const event = verdict.event;
      const first = await t.telemetry.recordWebhook({
        provider: psp.provider.id,
        eventId: event.providerEventId ?? `${event.reference}:${event.status}`,
        eventType: event.status,
        signatureValid: true,
        status: "received",
        reference: event.reference
      });
      if (!first) return { status: 200 };
      if (event.status !== "succeeded") return { status: 200 };
      const payment = await t.payments.byReference(event.reference);
      if (!payment) {
        await t.telemetry.recordWebhook({
          provider: psp.provider.id,
          eventId: `${event.providerEventId}:mismatch`,
          eventType: "tenant_mismatch",
          signatureValid: true,
          status: "tenant_mismatch",
          reference: event.reference,
          error: "Verified for this restaurant, but the reference belongs to another."
        });
        return { status: 200 };
      }
      const settled = await t.payments.settle({
        reference: event.reference,
        receivedCents: event.amountCents,
        currency: event.currency,
        providerRef: event.providerRef,
        ...event.paidAt ? { paidAt: event.paidAt } : {}
      });
      if (settled.kind === "currency_mismatch") {
        await t.telemetry.recordWebhook({
          provider: psp.provider.id,
          eventId: `${event.providerEventId}:currency`,
          eventType: "currency_mismatch",
          signatureValid: true,
          status: "currency_mismatch",
          reference: event.reference,
          error: `Expected ${settled.payment.currency}, got ${settled.got}.`
        });
        return { status: 200 };
      }
      if (settled.kind !== "unknown_reference") {
        await settleAndBroadcast(ctx, t, settled.payment.sessionId);
      }
      return { status: 200 };
    });
    res.sendStatus(outcome.status);
  };
  router.post("/psp/:webhookToken", express.raw({ type: "*/*", limit: "1mb" }), handler);
  router.post("/paystack/:webhookToken", express.raw({ type: "*/*", limit: "1mb" }), handler);
  return router;
}

// apps/server/src/routes/sentinel.ts
import { Router as Router6 } from "express";
var COOKIE2 = "sharyt_sentinel";
var SESSION_HOURS2 = 8;
var DUMMY_HASH2 = hashPassword("sentinel-timing-equaliser");
function sentinelRoutes(ctx) {
  const router = Router6();
  const cookieOptions = (req) => ({
    httpOnly: true,
    sameSite: "lax",
    secure: ctx.trustProxy ? true : req.protocol === "https",
    path: "/",
    maxAge: SESSION_HOURS2 * 36e5
  });
  function checkOrigin(req) {
    if (req.method === "GET" || req.method === "HEAD") return;
    const fetchSite = req.get("sec-fetch-site");
    if (fetchSite === "same-origin" || fetchSite === "none") return;
    if (fetchSite) throw ApiError.forbidden("Cross-site request refused.");
    const origin = req.get("origin");
    if (!origin) throw ApiError.forbidden("Missing Origin.");
    try {
      if (new URL(origin).host !== req.get("host")) throw new Error("mismatch");
    } catch {
      throw ApiError.forbidden("Cross-site request refused.");
    }
  }
  function requireOperator() {
    return async (req, _res, next) => {
      try {
        checkOrigin(req);
        const token = req.cookies?.[COOKIE2];
        if (!token) throw ApiError.unauthorized("Sign in.");
        const row = await ctx.db.withRegistry(
          (q) => q.one(
            `UPDATE platform_sessions s
                SET last_seen_at = now()
               FROM platform_users u
              WHERE s.token_hash = $1
                AND s.user_id = u.id
                AND s.expires_at > now()
                AND s.revoked_at IS NULL
                AND u.disabled_at IS NULL
              RETURNING u.id, u.email`,
            [hashToken(token)]
          )
        );
        if (!row) throw ApiError.unauthorized("Sign in.");
        req.operator = { id: row.id, email: row.email };
        next();
      } catch (err) {
        next(err);
      }
    };
  }
  router.get("/setup", async (_req, res) => {
    res.json({ ok: true, needsSetup: await operatorCount() === 0 });
  });
  router.post("/setup", async (req, res) => {
    checkOrigin(req);
    if (await operatorCount() > 0) {
      throw ApiError.conflict("This deployment already has an operator.");
    }
    const body = platformLoginSchema.parse(req.body);
    if (body.password.length < 12) {
      throw ApiError.badRequest("Use at least 12 characters -- this account can see every venue.");
    }
    await ctx.db.withRegistry(
      (q) => q("INSERT INTO platform_users (id, email, password_hash) VALUES ($1, $2, $3)", [
        newId("pu"),
        body.email.trim(),
        hashPassword(body.password)
      ])
    );
    res.status(201).json({ ok: true });
  });
  router.post("/login", async (req, res) => {
    checkOrigin(req);
    const body = platformLoginSchema.parse(req.body);
    const ip = req.ip ?? "unknown";
    if (!ctx.limiter.hit(platformKey("sentinel-login", ip), LIMITS.loginIp.limit, LIMITS.loginIp.windowMs).allowed) {
      throw ApiError.tooMany("Too many attempts. Wait a few minutes.");
    }
    const user = await ctx.db.withRegistry(
      (q) => q.one(
        "SELECT id, password_hash, email FROM platform_users WHERE email = $1 AND disabled_at IS NULL",
        [body.email.trim()]
      )
    );
    if (!user) {
      verifyPassword(body.password, DUMMY_HASH2);
      throw ApiError.unauthorized("That email and password do not match.");
    }
    if (!verifyPassword(body.password, user.password_hash)) {
      throw ApiError.unauthorized("That email and password do not match.");
    }
    const token = mintToken();
    await ctx.db.withRegistry(
      (q) => q(
        `INSERT INTO platform_sessions (id, user_id, token_hash, expires_at, ip, user_agent)
         VALUES ($1, $2, $3, now() + ($4 || ' hours')::interval, $5, $6)`,
        [newId("ps"), user.id, hashToken(token), String(SESSION_HOURS2), ip, req.get("user-agent") ?? null]
      )
    );
    await ctx.db.withRegistry(
      (q) => q("UPDATE platform_users SET last_login_at = now() WHERE id = $1", [user.id])
    );
    res.cookie(COOKIE2, token, cookieOptions(req));
    res.json({ ok: true, operator: { id: user.id, email: user.email } });
  });
  router.post("/logout", requireOperator(), async (req, res) => {
    const token = req.cookies[COOKIE2];
    await ctx.db.withRegistry(
      (q) => q("UPDATE platform_sessions SET revoked_at = now() WHERE token_hash = $1", [hashToken(token)])
    );
    res.clearCookie(COOKIE2, { path: "/" });
    res.json({ ok: true });
  });
  router.get("/me", requireOperator(), (req, res) => {
    res.json({ ok: true, operator: req.operator });
  });
  router.get("/overview", requireOperator(), async (_req, res) => {
    const totals = await platformTotals(ctx.db);
    const smtpConfigured = await ctx.mail.isConfigured();
    res.json({ ok: true, totals, smtpConfigured });
  });
  router.get("/restaurants", requireOperator(), async (_req, res) => {
    const rows = await ctx.db.withPlatform(
      (q) => q(
        `SELECT r.id, r.slug, r.name, r.status, r.plan, r.plan_status, r.currency,
                r.mock_mode, r.live_enabled_at, r.created_at, r.suspended_at,
                r.suspended_reason, r.platform_notes, r.trial_ends_at,
                (SELECT count(*) FROM tables tb
                  WHERE tb.restaurant_id = r.id AND tb.archived_at IS NULL) AS tables,
                (SELECT count(*) FROM restaurant_users u
                  WHERE u.restaurant_id = r.id AND u.disabled_at IS NULL) AS staff,
                (SELECT count(*) FROM sessions s WHERE s.restaurant_id = r.id) AS sessions_all,
                (SELECT count(*) FROM sessions s
                  WHERE s.restaurant_id = r.id
                    AND s.created_at > now() - interval '30 days') AS sessions_30d,
                (SELECT coalesce(sum(a.cost_micros), 0) FROM ai_usage a
                  WHERE a.restaurant_id = r.id) AS ai_cost_micros,
                (SELECT value FROM restaurant_settings st
                  WHERE st.restaurant_id = r.id AND st.key = 'psp_provider') AS provider
           FROM restaurants r
          ORDER BY r.created_at DESC`
      )
    );
    res.json({
      ok: true,
      restaurants: rows.map((r) => ({
        id: r.id,
        slug: r.slug,
        name: r.name,
        status: r.status,
        plan: r.plan,
        planStatus: r.plan_status,
        currency: r.currency,
        provider: r.provider ?? "mock",
        // The distinction the operator cares about: configured to take money,
        // or still simulating.
        live: !r.mock_mode && r.live_enabled_at !== null,
        tables: Number(r.tables),
        staff: Number(r.staff),
        sessionsAll: Number(r.sessions_all),
        sessions30d: Number(r.sessions_30d),
        aiCostMicros: Number(r.ai_cost_micros),
        createdAt: r.created_at,
        trialEndsAt: r.trial_ends_at,
        suspendedAt: r.suspended_at,
        suspendedReason: r.suspended_reason,
        notes: r.platform_notes
      }))
    });
  });
  router.put("/restaurants/:id/subscription", requireOperator(), async (req, res) => {
    const body = subscriptionSchema.parse(req.body);
    await ctx.db.withRegistry(
      (q) => q(
        `UPDATE restaurants
            SET plan = coalesce($2, plan),
                plan_status = coalesce($3, plan_status),
                platform_notes = coalesce($4, platform_notes)
          WHERE id = $1`,
        [String(req.params.id), body.plan ?? null, body.planStatus ?? null, body.notes ?? null]
      )
    );
    res.json({ ok: true });
  });
  router.put("/restaurants/:id/suspension", requireOperator(), async (req, res) => {
    const body = suspendSchema.parse(req.body);
    await ctx.db.withRegistry(
      (q) => q(
        `UPDATE restaurants
            SET suspended_at = CASE WHEN $2 THEN now() ELSE NULL END,
                suspended_reason = CASE WHEN $2 THEN $3 ELSE NULL END
          WHERE id = $1`,
        [String(req.params.id), body.suspended, body.reason ?? null]
      )
    );
    res.json({ ok: true });
  });
  router.get("/smtp", requireOperator(), async (_req, res) => {
    const settings = await ctx.mail.settings();
    res.json({
      ok: true,
      configured: settings !== null,
      // Never the password.
      host: settings?.host ?? "",
      port: settings?.port ?? 587,
      secure: settings?.secure ?? false,
      user: settings?.user ?? "",
      fromName: settings?.fromName ?? "Sharyt",
      fromAddress: settings?.fromAddress ?? "",
      passwordSet: Boolean(settings?.password)
    });
  });
  router.put("/smtp", requireOperator(), async (req, res) => {
    const body = smtpSettingsSchema.parse(req.body);
    await ctx.mail.saveSettings(body);
    res.json({ ok: true });
  });
  router.post("/smtp/test", requireOperator(), async (_req, res) => {
    const result = await ctx.mail.verifyConnection();
    if (!result.ok) throw ApiError.badRequest(result.reason ?? "Could not reach that mail server.");
    res.json({ ok: true });
  });
  return router;
  async function operatorCount() {
    const row = await ctx.db.withRegistry(
      (q) => q.one("SELECT count(*)::int AS n FROM platform_users")
    );
    return row?.n ?? 0;
  }
}

// apps/server/src/app.ts
var here2 = path3.dirname(fileURLToPath2(import.meta.url));
function webDistDir() {
  const bundled = path3.join(here2, "web");
  return existsSync3(path3.join(bundled, "index.html")) ? bundled : path3.resolve(here2, "../../web/dist");
}
function sentinelDistDir() {
  const bundled = path3.join(here2, "sentinel");
  return existsSync3(path3.join(bundled, "index.html")) ? bundled : path3.resolve(here2, "../../sentinel/dist");
}
function createApp(ctx) {
  const app = express2();
  app.set("trust proxy", ctx.trustProxy ? 1 : false);
  app.disable("x-powered-by");
  app.use("/api/webhooks", webhookRoutes(ctx));
  app.use(express2.json({ limit: "256kb" }));
  app.use(express2.urlencoded({ extended: false, limit: "64kb" }));
  app.use(cookieParser());
  app.use("/api/staff", staffRoutes(ctx));
  app.use("/api/sentinel", sentinelRoutes(ctx));
  app.get("/api/health", async (_req, res) => {
    res.json({
      ok: true,
      version: ctx.version,
      restaurants: await ctx.registry.count(),
      liveConnections: ctx.events.subscriberCount()
    });
  });
  app.use("/api", notFoundHandler);
  const dist = webDistDir();
  const hasBuild = existsSync3(path3.join(dist, "index.html"));
  app.get(
    "/:slug/t/:code",
    guardSlug,
    (req, res, next) => {
      if (!hasBuild || req.accepts(["json", "html"]) !== "html") return next();
      res.sendFile(path3.join(dist, "index.html"));
    }
  );
  app.use("/:slug/t/:code", guardSlug, tableRoutes(ctx));
  app.use("/:slug/mock", guardSlug, mockRoutes(ctx));
  app.use("/:slug/pay", guardSlug, payRoutes(ctx));
  const sentinelDist = sentinelDistDir();
  const hasSentinel = existsSync3(path3.join(sentinelDist, "index.html"));
  if (hasSentinel) {
    app.use("/sentinel", express2.static(sentinelDist, { index: false, redirect: false }));
    app.use("/sentinel", (_req, res) => {
      res.sendFile(path3.join(sentinelDist, "index.html"));
    });
  }
  if (hasBuild) {
    app.use(
      express2.static(dist, {
        index: false,
        setHeaders: (res, filePath) => {
          res.setHeader(
            "Cache-Control",
            filePath.includes(`${path3.sep}assets${path3.sep}`) ? "public, max-age=31536000, immutable" : "no-cache"
          );
        }
      })
    );
  }
  app.use((req, res) => {
    if (req.method !== "GET" && req.method !== "HEAD") {
      res.status(404).json({ ok: false, error: "No such route." });
      return;
    }
    if (!hasBuild) {
      res.status(503).type("html").send(
        `<!doctype html><meta charset="utf-8"><title>Not built</title>
         <body style="font-family:system-ui;max-width:40rem;margin:4rem auto;padding:0 1rem;line-height:1.6">
         <h1>The web app has not been built</h1>
         <p>Sharyt's API is running, but there is nothing in <code>apps/web/dist</code> to serve.</p>
         <p>Run <code>npm start</code> from the project root, which builds the front end first.</p>
         </body>`
      );
      return;
    }
    const page = req.path.startsWith("/admin") ? "admin.html" : "index.html";
    res.sendFile(path3.join(dist, page));
  });
  app.use(errorHandler);
  return app;
}
function guardSlug(req, res, next) {
  const slug = String(req.params.slug ?? "").toLowerCase();
  if (RESERVED_SLUGS.has(slug)) {
    res.status(404).json({ ok: false, error: "Not found." });
    return;
  }
  next();
}

// apps/server/src/context.ts
import path4 from "node:path";

// apps/server/src/db/payments.ts
var DuplicateCheckoutError = class extends Error {
};
var COLS = `id, session_id, participant_id, on_behalf_of, provider, reference,
              expected_cents, received_cents, currency, status, authorization_url,
              provider_ref, voided_at, created_at, paid_at`;
function map3(r) {
  return {
    id: r.id,
    sessionId: r.session_id,
    participantId: r.participant_id,
    onBehalfOf: r.on_behalf_of ?? null,
    provider: r.provider,
    reference: r.reference,
    expectedCents: r.expected_cents,
    receivedCents: r.received_cents ?? null,
    currency: r.currency,
    status: r.status,
    authorizationUrl: r.authorization_url ?? null,
    providerRef: r.provider_ref ?? null,
    voidedAt: iso(r.voided_at),
    createdAt: isoRequired(r.created_at),
    paidAt: iso(r.paid_at)
  };
}
var PaymentRepository = class {
  constructor(q, restaurantId) {
    this.q = q;
    this.restaurantId = restaurantId;
  }
  async byId(id) {
    const row = await this.q.one(`SELECT ${COLS} FROM payments WHERE id = $1`, [id]);
    return row ? map3(row) : null;
  }
  /**
   * Scoped, like everything else. Under row-level security a reference
   * belonging to another restaurant simply is not here -- which is what stops
   * one tenant verifying, or settling, another's transaction.
   */
  async byReference(reference) {
    const row = await this.q.one(`SELECT ${COLS} FROM payments WHERE reference = $1`, [reference]);
    return row ? map3(row) : null;
  }
  async forSession(sessionId) {
    const rows = await this.q(
      `SELECT ${COLS} FROM payments WHERE session_id = $1 ORDER BY created_at`,
      [sessionId]
    );
    return rows.map(map3);
  }
  /**
   * The checkout a diner currently holds, if any.
   *
   * `underpaid` is deliberately *not* live. It used to be, and nothing ever
   * cleared it: void skipped it, so no fresh checkout could be raised, and the
   * session could never reach its reconciliation target. One malformed webhook
   * -- a `charge.success` with a non-numeric amount defaulting to zero --
   * bricked that diner permanently. Underpayment now needs a cashier decision:
   * accept it, or void and reissue for the balance.
   */
  async liveForParticipant(sessionId, participantId) {
    const row = await this.q.one(
      `SELECT ${COLS} FROM payments
        WHERE session_id = $1 AND participant_id = $2
          AND status IN ('pending', 'paid') AND voided_at IS NULL
        ORDER BY created_at DESC LIMIT 1`,
      [sessionId, participantId]
    );
    return row ? map3(row) : null;
  }
  /**
   * One live checkout per person per session, enforced by a partial unique
   * index rather than by checking first.
   *
   * Two simultaneous taps on Pay both read "nothing live" and both insert. With
   * a synchronous driver the window was narrow; with an async one it is wide
   * open, and the reference carries a random suffix so the reference index does
   * not catch it. The result would be two payable links for one share.
   */
  async createPending(input) {
    try {
      const row = await this.q.one(
        `INSERT INTO payments (id, restaurant_id, session_id, participant_id, on_behalf_of,
                               provider, reference, expected_cents, currency, authorization_url)
         VALUES ($1, $2, $3, $4, $5, coalesce($6,'paystack'), $7, $8, $9, $10)
         RETURNING ${COLS}`,
        [
          newId("pay"),
          this.restaurantId,
          input.sessionId,
          input.participantId,
          input.onBehalfOf ?? null,
          input.provider ?? null,
          input.reference,
          input.expectedCents,
          input.currency,
          input.authorizationUrl
        ]
      );
      return map3(row);
    } catch (err) {
      if (String(err.message).includes("idx_payments_one_live")) {
        throw new DuplicateCheckoutError("That person already has a checkout open.");
      }
      throw err;
    }
  }
  /**
   * Money arrived.
   *
   * Three things are checked that were previously not:
   *
   * Currency, because per-tenant Paystack accounts make a mismatch plausible
   * rather than theoretical -- a ZAR bill settled by an NGN account matches on
   * the integer and closes a R400 table on about R4 of real money.
   *
   * The amount is required to be a number. It used to default to zero, so a
   * single malformed delivery wrote `underpaid` and stuck.
   *
   * A voided link still records what arrived. The transaction stays payable at
   * Paystack after we invalidate it locally, so the money is real and has to be
   * visible to someone; it just must not settle a share whose amount has moved.
   */
  async settle(input) {
    const existing = await this.byReference(input.reference);
    if (!existing) return { kind: "unknown_reference" };
    if (existing.status === "paid") return { kind: "already_paid", payment: existing };
    if (!Number.isFinite(input.receivedCents)) {
      throw new TypeError(`Refusing to settle ${input.reference}: amount was not a number.`);
    }
    if (input.currency && input.currency.toUpperCase() !== existing.currency.toUpperCase()) {
      return { kind: "currency_mismatch", payment: existing, got: input.currency.toUpperCase() };
    }
    const short = input.receivedCents < existing.expectedCents;
    const status = existing.voidedAt ? "voided" : short ? "underpaid" : "paid";
    const row = await this.q.one(
      `UPDATE payments
          SET received_cents = $2,
              provider_ref = coalesce($3, provider_ref),
              status = $4,
              paid_at = coalesce($5::timestamptz, now())
        WHERE id = $1 RETURNING ${COLS}`,
      [existing.id, input.receivedCents, input.providerRef ?? null, status, input.paidAt ?? null]
    );
    const payment = map3(row);
    if (existing.voidedAt) return { kind: "stale_link", payment };
    return short ? { kind: "underpaid", payment } : { kind: "applied", payment };
  }
  /** Any change to the bill kills every unpaid link, so nobody pays a stale amount. */
  async voidPending(sessionId) {
    const rows = await this.q(
      `UPDATE payments SET voided_at = now(), status = 'voided'
        WHERE session_id = $1 AND status = 'pending' AND voided_at IS NULL
        RETURNING id`,
      [sessionId]
    );
    return rows.length;
  }
  /**
   * The cashier's way out of an underpayment: void it so a fresh checkout can
   * be raised for the balance. Without this, `underpaid` is terminal.
   */
  async voidUnderpaid(paymentId) {
    await this.q(
      `UPDATE payments SET voided_at = now() WHERE id = $1 AND status = 'underpaid'`,
      [paymentId]
    );
  }
  /**
   * The reconciliation numbers, in one query, with the column named.
   *
   * Which column means "the money" was previously ambiguous -- the codebase
   * summed `expected_cents` in one place and `received_cents` in another, and
   * whichever got written first would have become the definition by accident.
   * It is `received_cents`: what we asked for is not evidence of what arrived.
   *
   * `underpaid` and `stale` are surfaced separately rather than folded in.
   * They are real money in the restaurant's account, so a shortfall computed
   * without them overstates the loss -- a R95 payment on a R100 share is R5
   * short, not R100.
   */
  async reconcile(sessionId) {
    const row = await this.q.one(
      `SELECT
         coalesce(sum(received_cents) FILTER (WHERE status = 'paid'  AND voided_at IS NULL), 0) AS paid,
         coalesce(sum(received_cents) FILTER (WHERE status = 'underpaid'), 0)                   AS underpaid,
         coalesce(sum(received_cents) FILTER (WHERE voided_at IS NOT NULL), 0)                  AS stale
       FROM payments WHERE session_id = $1`,
      [sessionId]
    );
    const paid = await this.q(
      `SELECT DISTINCT coalesce(on_behalf_of, participant_id) AS pid
         FROM payments WHERE session_id = $1 AND status = 'paid' AND voided_at IS NULL`,
      [sessionId]
    );
    return {
      paidCents: Number(row?.paid ?? 0),
      underpaidCents: Number(row?.underpaid ?? 0),
      staleCents: Number(row?.stale ?? 0),
      paidParticipants: paid.map((r) => r.pid)
    };
  }
  async markFailed(reference) {
    await this.q(
      `UPDATE payments SET status = 'failed' WHERE reference = $1 AND status = 'pending'`,
      [reference]
    );
  }
};

// apps/server/src/context.ts
var MissingDatabaseUrl = class extends Error {
};
async function createContext(options = {}) {
  const env = options.env ?? process.env;
  const dataDir = options.dataDir ?? env.DATA_DIR ?? path4.join(process.cwd(), "data");
  const connectionString = options.connectionString ?? env.DATABASE_URL;
  if (!connectionString) {
    throw new MissingDatabaseUrl(
      "DATABASE_URL is not set.\nSharyt needs Postgres -- tenant isolation is enforced by row-level security,\nwhich is a database feature, not something the application can fake.\nLocally: docker compose up -d, then copy .env.example to .env."
    );
  }
  const db = new Database({ connectionString });
  await db.migrate();
  const registry = new RegistryRepository(db);
  const events = new EventBus();
  const limiter = new RateLimiter();
  const resolved = resolveAppSecret(dataDir, env);
  const { key: appKey, source: appKeySource } = resolved ?? await appSecretFromDatabase(db);
  const mail = new MailService(db, appKey);
  const tenant = (restaurantId, fn) => db.withTenant(restaurantId, async (q) => {
    const restaurant = await registry.byId(restaurantId);
    if (!restaurant) throw new Error(`No such restaurant: ${restaurantId}`);
    return fn(buildRepos(q, restaurant));
  });
  return {
    db,
    registry,
    events,
    limiter,
    mail,
    dataDir,
    appKey,
    appKeySource,
    version: options.version ?? "1.0.0",
    publicUrl: env.PUBLIC_URL ?? null,
    // Default false. With this on and no proxy actually in front, req.ip comes
    // straight from a client-supplied header and every IP-keyed rate limit --
    // including the one throttling table-code guesses -- is spoofable.
    trustProxy: (env.TRUST_PROXY ?? "").toLowerCase() === "true",
    tenant,
    async tenantBySlug(slug, fn) {
      const restaurant = await registry.bySlug(slug);
      if (!restaurant) return null;
      return tenant(restaurant.id, fn);
    }
  };
}
function buildRepos(q, restaurant) {
  const id = restaurant.id;
  return {
    restaurantId: id,
    restaurant,
    q,
    sessions: new SessionRepository(q, id),
    payments: new PaymentRepository(q, id),
    tables: new TableRepository(q, id),
    staff: new StaffRepository(q, id),
    telemetry: new TelemetryRepository(q, id)
  };
}
async function appSecretFromDatabase(db) {
  const row = await db.withRegistry(async (q) => {
    await q(
      `INSERT INTO platform_settings (key, value, is_secret)
       VALUES ('app_secret', $1, TRUE)
       ON CONFLICT (key) DO NOTHING`,
      [newAppSecret()]
    );
    return q.one(
      "SELECT value FROM platform_settings WHERE key = 'app_secret'"
    );
  });
  if (!row?.value) throw new Error("Could not establish an application secret.");
  return { key: appSecretFrom(row.value), source: "database" };
}
async function closeContext(ctx) {
  ctx.events.closeAll();
  await ctx.db.close();
}

// apps/server/src/services/demo.ts
var DEMO_SLUG = "demo";
var DEMO_USERNAME = "admin";
var DEMO_PASSWORD = "kang2Paiko";
var MARKER_KEY = "demo.seeded";
async function seedDemo(ctx) {
  const existing = await ctx.db.withRegistry(
    (q) => q.one("SELECT value FROM platform_settings WHERE key = $1", [MARKER_KEY])
  );
  if (existing) {
    return { created: false, slug: DEMO_SLUG, username: DEMO_USERNAME };
  }
  const restaurantId = newId("r");
  await ctx.db.withRegistry(async (q) => {
    await q(
      `INSERT INTO restaurants
         (id, slug, slug_discriminator, name, status, currency, timezone,
          webhook_token, mock_mode, plan, platform_notes)
       VALUES ($1, $2, NULL, $3, 'active', 'ZAR', 'Africa/Johannesburg', $4, TRUE, 'trial', $5)`,
      [
        restaurantId,
        DEMO_SLUG,
        "Demo Bistro",
        `wht_${mintToken()}${mintToken()}`,
        "Seeded demo venue. Safe to suspend or delete."
      ]
    );
    await q("INSERT INTO retired_slugs (slug, restaurant_id) VALUES ($1, $2)", [DEMO_SLUG, restaurantId]);
  });
  await ctx.tenant(restaurantId, async (t) => {
    const staff = [
      [DEMO_USERNAME, DEMO_PASSWORD, "owner"],
      ["manager@demo.local", DEMO_PASSWORD, "manager"],
      ["cashier@demo.local", DEMO_PASSWORD, "cashier"]
    ];
    for (const [email, password, role] of staff) {
      await t.q(
        `INSERT INTO restaurant_users
           (id, restaurant_id, email, password_hash, role, email_verified_at)
         VALUES ($1, $2, $3, $4, $5, now())`,
        [newId("usr"), restaurantId, email, hashPassword(password), role]
      );
    }
    const tableIds = {};
    for (const [code, seats, label] of [
      ["1", 2, "Window"],
      ["2", 2, "Window"],
      ["3", 4, null],
      ["4", 4, null],
      ["5", 6, "Corner booth"],
      ["6", 8, "Long table"],
      ["B1", 2, "Bar"],
      ["B2", 2, "Bar"]
    ]) {
      const id = newId("tb");
      tableIds[code] = id;
      await t.q(
        "INSERT INTO tables (id, restaurant_id, code, label, seats) VALUES ($1, $2, $3, $4, $5)",
        [id, restaurantId, code, label, seats]
      );
    }
    for (const [key, value] of [
      ["brand.display_name", "Demo Bistro"],
      ["brand.closing_note", "Thanks for eating with us."],
      ["tip.presets", "[0, 10, 12.5, 15]"],
      ["tip.default", "12.5"]
    ]) {
      await t.q(
        `INSERT INTO restaurant_settings (restaurant_id, key, value)
         VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
        [restaurantId, key, value]
      );
    }
    const codes = Object.keys(tableIds);
    let n = 0;
    for (let daysAgo = 30; daysAgo >= 1; daysAgo--) {
      const weekend = daysAgo % 7 === 0 || daysAgo % 7 === 6;
      const covers = weekend ? 5 : 2;
      for (let i = 0; i < covers; i++) {
        n += 1;
        const code = codes[n % codes.length];
        const heads = 2 + n % 5;
        const foodCents = 18e3 + n * 7300 % 62e3;
        const tipPct = [0, 10, 10, 12.5, 15][n % 5];
        const tipCents = Math.round(foodCents * tipPct / 100);
        const total = foodCents + tipCents;
        const walkout = n % 19 === 0;
        const short = !walkout && n % 23 === 0;
        const received = walkout ? Math.round(total / heads) : short ? total - 4500 : total;
        const sessionId = newId("ses");
        await t.q(
          `INSERT INTO sessions
             (id, restaurant_id, table_id, status, pay_mode, headcount, headcount_locked_at,
              currency, items_cents, tip_cents, bill_total_cents, bill_frozen_at,
              short_cents, short_reason, created_at, updated_at, last_activity_at, closed_at)
           VALUES ($1, $2, $3, $4, 'equal', $5, $6, 'ZAR', $7, $8, $9, $6,
                   $10, $11, $6, $6, $6, $12)`,
          [
            sessionId,
            restaurantId,
            tableIds[code],
            walkout ? "short" : "paid",
            heads,
            daysAgoAt(daysAgo, 19 + n % 3),
            foodCents,
            tipCents,
            total,
            walkout ? total - received : null,
            walkout ? "Walked out before settling" : null,
            daysAgoAt(daysAgo, 21 + n % 3)
          ]
        );
        const participantId = newId("par");
        await t.q(
          `INSERT INTO session_participants
             (id, restaurant_id, session_id, name, is_host, seat_no, joined_at)
           VALUES ($1, $2, $3, $4, TRUE, 1, $5)`,
          [participantId, restaurantId, sessionId, HOST_NAMES[n % HOST_NAMES.length], daysAgoAt(daysAgo, 19)]
        );
        await t.q(
          `INSERT INTO payments
             (id, restaurant_id, session_id, participant_id, provider, reference,
              expected_cents, received_cents, currency, status, created_at, paid_at)
           VALUES ($1, $2, $3, $4, 'mock', $5, $6, $7, 'ZAR', $8, $9, $9)`,
          [
            newId("pay"),
            restaurantId,
            sessionId,
            participantId,
            `${restaurantId}_demo_${n}`,
            total,
            received,
            short ? "underpaid" : "paid",
            daysAgoAt(daysAgo, 21)
          ]
        );
      }
    }
    await t.q(
      `INSERT INTO sessions (id, restaurant_id, table_id, status, currency)
       VALUES ($1, $2, $3, 'open', 'ZAR')`,
      [newId("ses"), restaurantId, tableIds["5"]]
    );
  });
  await ctx.db.withRegistry(
    (q) => q("INSERT INTO platform_settings (key, value) VALUES ($1, $2)", [MARKER_KEY, restaurantId])
  );
  return { created: true, slug: DEMO_SLUG, username: DEMO_USERNAME };
}
async function demoStillOpen(ctx) {
  const marker = await ctx.db.withRegistry(
    (q) => q.one("SELECT value FROM platform_settings WHERE key = $1", [MARKER_KEY])
  );
  if (!marker) return false;
  const row = await ctx.db.withPlatform(
    (q) => q.one(
      `SELECT count(*)::int AS n
         FROM restaurant_users
        WHERE restaurant_id = $1 AND email = $2 AND disabled_at IS NULL`,
      [marker.value, DEMO_USERNAME]
    )
  );
  return (row?.n ?? 0) > 0;
}
var HOST_NAMES = ["Thabo", "Lerato", "Naledi", "Sipho", "Ayanda", "Bongi", "Zanele", "Kagiso"];
function daysAgoAt(daysAgo, hour) {
  const d = /* @__PURE__ */ new Date();
  d.setUTCDate(d.getUTCDate() - daysAgo);
  d.setUTCHours(hour, 30, 0, 0);
  return d.toISOString();
}

// apps/server/src/index.ts
async function main() {
  const port = Number(process.env.PORT ?? 3e3);
  let ctx;
  try {
    ctx = await createContext();
  } catch (err) {
    console.error(`
${err.message}
`);
    process.exit(1);
  }
  if ((process.env.SEED_DEMO ?? "true").toLowerCase() !== "false") {
    try {
      const demo = await seedDemo(ctx);
      if (demo.created) {
        console.log(`Seeded the demo venue at /${demo.slug} (sign in as ${demo.username}).`);
      }
    } catch (err) {
      console.error(`[demo] could not seed: ${err.message}`);
    }
  }
  const server = createApp(ctx).listen(port, async () => {
    console.log(`sharyt listening on http://localhost:${port}`);
    if (ctx.appKeySource === "database") {
      console.warn(
        "\n  The application secret is stored in the database, because no APP_SECRET\n  was set and no writable data directory was available. Payment credentials\n  and two-factor secrets are encrypted with a key that lives beside them.\n  Set APP_SECRET in the environment if this deployment supports it.\n"
      );
    }
    const count = await ctx.registry.count();
    if (count === 0) console.log("No restaurants yet. POST /api/staff/signup to create one.");
    if (await demoStillOpen(ctx)) {
      console.warn(
        `
  The demo venue is live at /${DEMO_SLUG} with the published password (${DEMO_USERNAME} / ${DEMO_PASSWORD}).
  Fine while you are showing the product. Before real restaurants use this
  deployment, change that password or suspend the venue from Sentinel.
`
      );
    }
  });
  const shutdown = (signal) => {
    console.log(`
${signal} -- shutting down`);
    server.close(() => {
      void closeContext(ctx).finally(() => process.exit(0));
    });
    setTimeout(() => process.exit(1), 5e3).unref();
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}
void main();
