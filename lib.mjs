// Fate's Hand — table server, pure logic (no I/O, no side effects).
// Kept separate from table-server.mjs so this can be unit-tested without a
// live server, a tunnel, or a network call — see tests/lib.test.js.
//
// Design: ~/tools/fh-phb/COMPANION-BUILD-PLAN.md §12. This file implements
// it; it does not decide it. The event shapes and settlement rules are
// frozen at plan §11 and carried over unchanged (§12.1) — only the transport
// moves.

import crypto from "node:crypto";

export const FEED_TYPES = new Set(["roll", "note"]);
export const INTENT_KINDS = new Set(["check", "damage", "spell"]);

export const clean = (s) => (typeof s === "string" ? s.trim().slice(0, 200) : "");
export const cleanText = (s, max = 4000) => (typeof s === "string" ? s.trim().slice(0, max) : "");
export const clampInt = (value, min, max, fallback = min) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.round(n)));
};
export const withinBytes = (value, maxBytes) => {
  try {
    return Buffer.byteLength(JSON.stringify(value), "utf8") <= maxBytes;
  } catch {
    return false;
  }
};

export const safeIntent = (value) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  if (!INTENT_KINDS.has(value.kind)) return null;
  return withinBytes(value, 2_000) ? value : null;
};

// Mirrors fh-worker/src/worker.js safeFeedEvent (~lines 964-991) in shape, so
// an event this server stores is the same thing the Worker would have
// produced from the same POST body.
export const safeFeedEvent = (body, campaign) => {
  const type = FEED_TYPES.has(body?.type) ? body.type : null;
  if (!type) return null;
  const pseudo = clean(body?.actor?.pseudo);
  if (!pseudo) return null;
  const display =
    body?.display && typeof body.display === "object" && !Array.isArray(body.display) && withinBytes(body.display, 6_000)
      ? body.display
      : null;
  if (type === "roll" && !display) return null;
  const ddbCharacterId = Number(body?.actor?.ddbCharacterId);
  return {
    schema: "fh-event/1",
    id: cleanText(String(body?.id || crypto.randomUUID()), 80) || crypto.randomUUID(),
    ts: new Date().toISOString(),
    campaign,
    actor: {
      pseudo,
      character: clean(body?.actor?.character) || pseudo,
      ddbCharacterId: Number.isSafeInteger(ddbCharacterId) && ddbCharacterId > 0 ? ddbCharacterId : null,
    },
    type,
    // A roll revised in place (a bonus die staged onto an open roll) reappends
    // under the same rollId with a higher rev. Readers key on rollId and keep
    // the highest rev — see plan §11.4b. Unchanged here.
    rollId: cleanText(String(body?.rollId || ""), 80) || null,
    rev: clampInt(body?.rev, 0, 999, 0),
    display,
    intent: safeIntent(body?.intent),
  };
};

// The seq IS the timestamp (plan §11.4 / §12.3): 13-digit zero-padded epoch ms
// plus a 4-char random tiebreaker, so lexicographic order matches chronological
// order — kept identical to the cloud Worker's format so a dock's cursor code
// never needs to know which source it is talking to. A factory, not a
// module-level singleton, so tests get a fresh sequence per case. Guards
// against the clock going backwards or two calls landing in the same tick.
export const makeSeqGenerator = (clock = Date.now) => {
  let last = "";
  const msOf = (seq) => Number(String(seq).split("-")[0]) || 0;
  return () => {
    const ms = Math.max(clock(), msOf(last) + 1);
    const seq = `${String(ms).padStart(13, "0")}-${crypto.randomBytes(3).toString("hex").slice(0, 4)}`;
    last = seq;
    return seq;
  };
};

// A ring buffer entry is {seq, event}. Mirrors the Worker's readFeed
// (worker.js ~997-1010): events strictly after `since`, most-recent `limit`
// of those, cursor is the last one returned — never invent a cursor when
// nothing new arrived, just echo `since` back.
export const sinceSlice = (buffer, since, limit) => {
  const rows = since ? buffer.filter((row) => row.seq > since) : buffer;
  const wanted = rows.slice(-limit);
  return {
    events: wanted.map((row) => row.event),
    cursor: wanted.length ? wanted[wanted.length - 1].seq : since || "",
  };
};

// Mirrors fh-worker/src/worker.js ADMIN_ORIGINS/DEV_ORIGINS (~lines 38-39).
// The dock is cross-origin from this server (github.io vs *.trycloudflare.com
// or 127.0.0.1), and the POST carries a JSON Content-Type — that triggers a
// real preflight (plan §12.3 / §12.9), not just a bare Allow-Origin.
export const ADMIN_ORIGINS = ["https://noirchicot.github.io"];
export const DEV_ORIGINS = [/^http:\/\/localhost:\d{2,5}$/, /^http:\/\/127\.0\.0\.1:\d{2,5}$/];
// Package 12b connects from a Manifest V3 service worker on the DM's own
// machine. Unpacked extensions do not have a stable id, so the loopback
// server accepts Chrome's syntactically valid extension origins as a class.
// The campaign code remains the membership gate and the socket still binds
// only to 127.0.0.1; unrelated web origins remain refused.
export const EXTENSION_ORIGINS = [/^chrome-extension:\/\/[a-p]{32}$/];

export const isAllowedOrigin = (origin) =>
  !!origin &&
  (ADMIN_ORIGINS.includes(origin) ||
    DEV_ORIGINS.some((re) => re.test(origin)) ||
    EXTENSION_ORIGINS.some((re) => re.test(origin)));

export const timingSafeEqual = (a, b) => {
  const bufA = crypto.createHash("sha256").update(String(a ?? "")).digest();
  const bufB = crypto.createHash("sha256").update(String(b ?? "")).digest();
  return crypto.timingSafeEqual(bufA, bufB);
};

// A simple fixed-window limiter, same shape as the Worker's feedRateLimited
// (worker.js ~1011-1021) but in memory instead of KV — this process has no KV.
export const makeRateLimiter = (limit = 90, windowMs = 60_000, clock = Date.now) => {
  const buckets = new Map();
  return (ip) => {
    const key = `${ip}|${Math.floor(clock() / windowMs)}`;
    const used = (buckets.get(key) || 0) + 1;
    buckets.set(key, used);
    if (buckets.size > 10_000) buckets.clear(); // defensive: never grow unbounded
    return used > limit;
  };
};

// ---------- character ownership (plan §13.13) ----------
// Pure logic only, same reason as the rest of this file: unit-testable
// without a server, an archive directory, or a network call.

export const hasOwn = (object, key) => Object.prototype.hasOwnProperty.call(object || {}, key);

// A pseudo becomes a JSON filename ({pseudo}.json) and an HTTP path segment,
// so it needs the Worker's *stricter* charset (mirrors fh-worker/src/worker.js
// `clean`, ~lines 67-68) rather than the freer `clean` above — anything
// outside [letter, number, space, _, -] must never reach path.join, or a
// pseudo like "../../etc" becomes a path-traversal bug instead of a rejected
// request.
export const cleanPseudo = (s) =>
  typeof s === "string" && /^[\p{L}\p{N} _-]{1,40}$/u.test(s.trim()) ? s.trim() : null;

// Mirrors fh-worker/src/worker.js defaultCharacterProfile + publicCharacterProfile
// (~lines 847-899) collapsed into one shape: the table server only ever stores
// and serves the public form, never the Worker-internal `ddb.linkedAt` wrapper
// — nothing on this server's routes needs it (the DDB pull route is not part
// of plan §13.13's scope here; see the build plan).
export const defaultCharacterProfile = (clock = Date.now) => ({
  schemaVersion: 1,
  ddbLinked: false,
  characterId: null,
  snapshot: null,
  preparation: { transferEssence: false, identify: false, tools: [] },
  levelUps: [],
  destinyState: null,
  vitalsState: null,
  rollHistory: [],
  rollEvents: [],
  rollPrefs: null,
  manualOverrides: null,
  pendingRoll: null,
  revision: 0,
  updatedAt: new Date(clock()).toISOString(),
});

// Verbatim port of fh-worker/src/worker.js safeOpaque (~lines 833-845): bounds
// shape and byte size only, no domain knowledge. Deliberately NOT extended to
// match safePreparation/safeLevelUps (~lines 800-827) too, which lean on
// ABILITIES/ESSENTIAL_SKILLS — game-content lists that belong to the Worker.
// Duplicating those here would be a second copy that silently drifts the day
// either list changes there. A well-formed patch (the only kind the dock ever
// sends) is untouched by that gap; a malformed one is still bounded by the
// caps below, and gets the Worker's own stricter shape check for free the
// moment it mirrors there.
export const safeOpaque = (value, { array = false, maxBytes = 8_000, maxItems = 50 } = {}) => {
  const fits = (x) => {
    try {
      return JSON.stringify(x).length <= maxBytes;
    } catch {
      return false;
    }
  };
  if (array) {
    if (!Array.isArray(value)) return [];
    const list = value.slice(0, maxItems);
    return fits(list) ? list : [];
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return fits(value) ? value : null;
};

// Same nine keys and the same per-key budgets as fh-worker's
// POST /profile/:code/:pseudo (~lines 1162-1177). `preparation` and
// `levelUps` get the generic bound only — see safeOpaque's comment above.
const PROFILE_PATCH = {
  preparation: (v) => safeOpaque(v, { maxBytes: 2_000 }),
  levelUps: (v) => safeOpaque(v, { array: true, maxItems: 20, maxBytes: 20_000 }),
  destinyState: (v) => safeOpaque(v),
  vitalsState: (v) => safeOpaque(v, { maxBytes: 1_000 }),
  rollHistory: (v) => safeOpaque(v, { array: true, maxItems: 20, maxBytes: 40_000 }),
  rollEvents: (v) => safeOpaque(v, { array: true, maxItems: 10, maxBytes: 8_000 }),
  rollPrefs: (v) => safeOpaque(v, { maxBytes: 4_000 }),
  manualOverrides: (v) => safeOpaque(v, { maxBytes: 8_000 }),
  pendingRoll: (v) => safeOpaque(v, { maxBytes: 8_000 }),
};

// The plan §13.13.2 rule, applied to a profile patch: a write must name the
// revision it is based on (absent is treated as 0); it is accepted only if
// that matches the record's current revision, which is then stamped +1.
// Anything else is a 409 carrying the record as it actually stands right
// now — never a silent overwrite, never a silently dropped write. Pure: the
// caller owns loading `profile` and persisting the result.
export const applyProfilePatch = (profile, body, clock = Date.now) => {
  const applied = Object.keys(PROFILE_PATCH).filter((key) => hasOwn(body, key));
  if (!applied.length) return { ok: false, status: 400, body: { error: "nothing to update" } };
  const currentRevision = Number.isSafeInteger(profile?.revision) ? profile.revision : 0;
  const suppliedRevision = clampInt(body.revision, 0, 1_000_000_000, 0);
  if (suppliedRevision !== currentRevision) {
    return { ok: false, status: 409, body: { error: "conflict", currentRevision, current: profile } };
  }
  const next = { ...profile };
  applied.forEach((key) => {
    next[key] = PROFILE_PATCH[key](body[key]);
  });
  next.revision = currentRevision + 1;
  next.updatedAt = new Date(clock()).toISOString();
  return { ok: true, profile: next };
};

// Matches fh-worker's POST /builds cap (~line 1091): the build blob itself,
// not the whole request body, is capped at 200,000 JSON characters.
const MAX_BUILD_CHARS = 200_000;

// The plan §13.13.2 rule again, for the build record instead of the profile.
// `existing` is the full stored record ({pseudo, campaign, updatedAt,
// revision, build}) or null for a pseudo this archive has never seen — which
// naturally makes revision 0 the only accepted value for a brand-new
// character, exactly as it does on the Worker.
export const applyBuildWrite = (existing, pseudo, body, clock = Date.now) => {
  if (!body?.build) return { ok: false, status: 400, body: { error: "pseudo and build are required" } };
  let buildLength;
  try {
    buildLength = JSON.stringify(body.build).length;
  } catch {
    return { ok: false, status: 400, body: { error: "invalid build" } };
  }
  if (buildLength > MAX_BUILD_CHARS) return { ok: false, status: 413, body: { error: "build too large" } };
  const currentRevision = Number.isSafeInteger(existing?.revision) ? existing.revision : 0;
  const suppliedRevision = clampInt(body.revision, 0, 1_000_000_000, 0);
  if (suppliedRevision !== currentRevision) {
    return { ok: false, status: 409, body: { error: "conflict", currentRevision, current: existing } };
  }
  const record = {
    pseudo,
    campaign: body.campaign,
    updatedAt: new Date(clock()).toISOString(),
    revision: currentRevision + 1,
    build: body.build,
  };
  return { ok: true, record };
};
