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

export const isAllowedOrigin = (origin) =>
  !!origin && (ADMIN_ORIGINS.includes(origin) || DEV_ORIGINS.some((re) => re.test(origin)));

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
