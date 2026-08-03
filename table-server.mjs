#!/usr/bin/env node
// Fate's Hand — table server (plan §12 / package 12a; character ownership
// added under plan §13.13).
//
// Run on the DM's own machine during a session. Hosts the LIVE campaign feed
// for one campaign at a time: an SSE stream for delivery, the same GET/POST
// contract the cloud Worker exposes at /feed, mirrored onward to that Worker
// as a backstop. It also owns that campaign's character records while it
// runs (plan §13.13): a local JSON archive under --archive-dir, pulled from
// the cloud Worker at startup and mirrored back to it on every write. See
// ~/tools/fh-phb/COMPANION-BUILD-PLAN.md §12 and §13.13 for the design and
// the "why" behind every decision below — this file implements it, it does
// not decide it.
//
// Everything above the transport is unchanged from plan §11: settlement at
// openRollState, rollId+rev revisions, dedupe by id, the fh-event/1 and
// fh-roll/1 shapes. This process only changes where events are stored and
// how a dock reaches them.
//
// Usage:
//   node table-server.mjs <CAMPAIGN-CODE> [--port 8791] [--no-mirror]
//     [--no-tunnel] [--archive-dir ~/fh-archive] [--worker-origin URL]
//
// Zero npm dependencies, on purpose (§12.8): this runs moments before a game,
// players waiting, and must never fail on an install.

import http from "node:http";
import https from "node:https";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  safeFeedEvent,
  makeSeqGenerator,
  sinceSlice,
  isAllowedOrigin,
  timingSafeEqual,
  makeRateLimiter,
  cleanText,
  clampInt,
  cleanPseudo,
  defaultCharacterProfile,
  applyProfilePatch,
  applyBuildWrite,
  attachActorAvatar,
} from "./lib.mjs";
import { encodeFrame, decodeFrames, handshakeResponse, OPCODE } from "./ws.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------- config ----------
const args = process.argv.slice(2);
const CODE = args.find((a) => !a.startsWith("--"));
if (!CODE) {
  console.error(
    "usage: node table-server.mjs <CAMPAIGN-CODE> [--port 8791] [--no-mirror] [--no-tunnel] " +
      "[--archive-dir ~/fh-archive] [--worker-origin URL]",
  );
  process.exit(1);
}
const flag = (name) => args.includes(`--${name}`);
const opt = (name, fallback) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};
const PORT = Number(opt("port", "8791"));
// --no-mirror means no cloud relationship at all for this run: neither the
// feed backstop (plan §12.6) nor, now, the character-ownership sync and
// mirror (plan §13.13) — a deliberate reuse of one flag rather than adding a
// second one that means almost the same thing.
const MIRROR = !flag("no-mirror");
const TUNNEL = !flag("no-tunnel");
const WORKER_ORIGIN = opt("worker-origin", "https://fh-builds.noirchicot.workers.dev");
const DATA_DIR = path.join(__dirname, "data");
const RING_MAX = 400; // plan §12.7 — generous for one session
const MAX_BODY_BYTES = 8_000; // plan §12.9, matches the Worker's safeOpaque caps
// The Worker caps a build at 200,000 JSON characters (plan §13.13); this
// leaves headroom for the pseudo/campaign/revision wrapper around it.
const MAX_CHARACTER_BODY_BYTES = 220_000;

// GM_TOKEN — the same secret the cloud Worker uses (`wrangler secret put
// GM_TOKEN`), needed to register the rendezvous record and to mirror events.
// Loaded from the environment first, then a local .secrets.local
// (KEY=VALUE, gitignored — same shape as fh-worker/.secrets.local).
const loadSecrets = () => {
  const file = path.join(__dirname, ".secrets.local");
  const out = {};
  if (fs.existsSync(file)) {
    for (const line of fs.readFileSync(file, "utf8").split("\n")) {
      const m = /^([A-Z_]+)=(.*)$/.exec(line.trim());
      if (m) out[m[1]] = m[2];
    }
  }
  return out;
};
const secrets = loadSecrets();
const GM_TOKEN = process.env.GM_TOKEN || secrets.GM_TOKEN || "";

// ---------- terminal panel (plan §12.8 — must never require reading a log
// to learn the table is down) ----------
const color = (code, text) => `\x1b[${code}m${text}\x1b[0m`;
const log = (msg, level = "info") => {
  const tag = { info: color(36, "[table]"), warn: color(33, "[table]"), error: color(31, "[table]") }[level] || "[table]";
  console.log(`${tag} ${msg}`);
};

const startedAt = new Date().toISOString();
let tunnelUrl = null;
let tunnelHealthy = !TUNNEL; // --no-tunnel: nothing to be unhealthy about
let heartbeatOk = null; // null = not yet attempted

const printStatus = () => {
  const statusBits = [];
  if (TUNNEL && !tunnelHealthy) statusBits.push(color(31, "tunnel DOWN"));
  statusBits.push(`${wsClients.size + sseClients.size} connected`);
  statusBits.push(`${buffer.length} events`);
  // Rendezvous (can players find this table?) and mirroring (is the cloud
  // backstop getting a copy?) are independent — plan §12.4 vs §12.6.
  if (heartbeatOk === false) statusBits.push(color(31, "rendezvous FAILING"));
  else if (heartbeatOk === true) statusBits.push("rendezvous ✓");
  statusBits.push(MIRROR ? "mirroring ✓" : "mirroring off");
  // Character ownership (plan §13.13): archiveSynced/unmirroredWrites are
  // declared further down this file but already initialized by the time this
  // function is ever called (only from async callbacks and timers, never
  // during the module's own top-to-bottom evaluation).
  statusBits.push(archiveSynced ? `${archive.size} character(s) synced` : color(33, "characters NOT synced"));
  if (unmirroredWrites > 0) statusBits.push(color(31, `${unmirroredWrites} character write(s) not on cloud`));
  console.log(
    [
      "",
      "  FATE'S HAND — TABLE SERVER",
      `  campaign   ${CODE}`,
      `  players    ${tunnelUrl || (TUNNEL ? "waiting for tunnel…" : "--no-tunnel: give players a manual URL")}`,
      `  status     ${statusBits.join(" · ")}`,
      "",
    ].join("\n"),
  );
};

// ---------- storage: in-memory ring buffer + append-only JSONL (plan §12.7) ----------
// Across a reconnect: the ring buffer + Last-Event-ID (SSE handles this on
// its own). Across a restart: replay the tail of the JSONL. Across sessions:
// nothing — plan §11.6 already settled that this is a table log, not an
// archive; the JSONL is a by-product (a free session transcript), not a
// feature with a retention policy.
fs.mkdirSync(DATA_DIR, { recursive: true });
const buffer = []; // [{seq, event}], oldest first
const nextSeq = makeSeqGenerator();

const dayFile = (daysAgo = 0) => {
  const d = new Date(Date.now() - daysAgo * 86_400_000).toISOString().slice(0, 10);
  return path.join(DATA_DIR, `${CODE}-${d}.jsonl`);
};
const appendJsonl = (seq, event) => {
  fs.appendFile(dayFile(), `${JSON.stringify({ seq, event })}\n`, (err) => {
    if (err) log(`could not write session log: ${err.message}`, "warn");
  });
};
const replayRecent = () => {
  const cutoff = Date.now() - 4 * 60 * 60 * 1000;
  const rows = [];
  for (const daysAgo of [1, 0]) {
    const file = dayFile(daysAgo);
    if (!fs.existsSync(file)) continue;
    for (const line of fs.readFileSync(file, "utf8").split("\n")) {
      if (!line.trim()) continue;
      try {
        const row = JSON.parse(line);
        if (new Date(row.event.ts).getTime() >= cutoff) rows.push(row);
      } catch {
        // a torn last line from a previous crash — skip it, do not fail startup
      }
    }
  }
  rows.sort((a, b) => (a.seq < b.seq ? -1 : a.seq > b.seq ? 1 : 0));
  for (const row of rows.slice(-200)) buffer.push(row);
  if (rows.length) log(`replayed ${Math.min(200, rows.length)} recent event(s) from the session log`);
};
replayRecent();

// ---------- subscribers + publish ----------
// WebSocket is the production delivery path (plan §12.3 as amended §12.11):
// Cloudflare Quick Tunnel delivers no streamed HTTP body, so SSE cannot reach a
// remote player. SSE is kept because it works perfectly over loopback and
// `curl -N` on it is the fastest way to eyeball a live feed — debugging and the
// bridge, never a remote dock.
const sseClients = new Set();
const wsClients = new Set(); // net.Socket, already upgraded

const publish = (event) => {
  const seq = nextSeq();
  buffer.push({ seq, event });
  if (buffer.length > RING_MAX) buffer.shift();
  appendJsonl(seq, event);

  const ssePayload = `id: ${seq}\nevent: fh-event\ndata: ${JSON.stringify(event)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(ssePayload);
    } catch {
      sseClients.delete(client);
    }
  }

  const wsPayload = encodeFrame(JSON.stringify({ seq, event }));
  for (const socket of wsClients) {
    try {
      socket.write(wsPayload);
    } catch {
      wsClients.delete(socket);
    }
  }

  if (MIRROR && GM_TOKEN) mirror(event);
  return seq;
};

// ---------- outbound HTTP(S) helpers (to the cloud Worker) ----------
// Protocol and port follow the URL rather than assuming https/443: the real
// Worker is always https, so production behaviour is unchanged, but plan
// §13.13's tests point --worker-origin at a plain-http loopback stand-in, and
// without this these calls would silently try to speak TLS to it (or land on
// the wrong port) instead of failing usefully.
const httpClientFor = (u) => (u.protocol === "https:" ? https : http);

const postJson = (url, body, headers = {}) =>
  new Promise((resolve, reject) => {
    const data = Buffer.from(JSON.stringify(body));
    const u = new URL(url);
    const req = httpClientFor(u).request(
      {
        hostname: u.hostname,
        port: u.port || undefined,
        path: u.pathname + u.search,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": data.length, ...headers },
      },
      (res) => {
        let chunks = "";
        res.on("data", (c) => (chunks += c));
        res.on("end", () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            try {
              resolve(JSON.parse(chunks));
            } catch {
              resolve({});
            }
          } else {
            reject(new Error(`HTTP ${res.statusCode}: ${chunks.slice(0, 200)}`));
          }
        });
      },
    );
    req.on("error", reject);
    req.write(data);
    req.end();
  });

// Same shape as postJson, GET instead of POST — plan §13.13.3's startup pull
// (party/build/profile) reads, it never writes.
const getJson = (url, timeoutMs = 10_000) =>
  new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = httpClientFor(u).request(
      { hostname: u.hostname, port: u.port || undefined, path: u.pathname + u.search, method: "GET" },
      (res) => {
        let chunks = "";
        res.on("data", (c) => (chunks += c));
        res.on("end", () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            try {
              resolve(JSON.parse(chunks));
            } catch (err) {
              reject(new Error(`invalid JSON from ${url}: ${err.message}`));
            }
          } else {
            reject(new Error(`HTTP ${res.statusCode} from ${url}: ${chunks.slice(0, 200)}`));
          }
        });
      },
    );
    req.on("error", reject);
    req.setTimeout(timeoutMs, () => req.destroy(new Error(`request to ${url} timed out after ${timeoutMs}ms`)));
    req.end();
  });

const deleteTable = () => {
  const u = new URL(`${WORKER_ORIGIN}/table/${encodeURIComponent(CODE)}`);
  const req = httpClientFor(u).request({
    hostname: u.hostname,
    port: u.port || undefined,
    path: u.pathname,
    method: "DELETE",
    headers: { Authorization: `Bearer ${GM_TOKEN}` },
  });
  req.on("error", () => {}); // best-effort — the TTL clears it anyway (plan §12.4)
  req.end();
};

// Mirror is fire-and-forget from publish()'s point of view: the roll already
// landed locally and every dock in LIVE mode already has it. What mirroring
// buys is the RECENT-state backstop (plan §12.6) and off-site durability.
// Never batched — the Worker re-stamps `ts` on arrival, so batching would
// silently skew the backstop's timestamps.
const mirror = (event) => {
  postJson(`${WORKER_ORIGIN}/feed/${encodeURIComponent(CODE)}`, event).catch((err) => {
    log(`mirror to cloud backstop failed: ${err.message}`, "warn");
  });
};

const registerTable = () => {
  if (!tunnelUrl) return;
  if (!GM_TOKEN) {
    log("no GM_TOKEN (set env GM_TOKEN or create .secrets.local) — skipping rendezvous; give players the URL directly", "warn");
    return;
  }
  postJson(`${WORKER_ORIGIN}/table/${encodeURIComponent(CODE)}`, { url: tunnelUrl }, { Authorization: `Bearer ${GM_TOKEN}` })
    .then(() => {
      const wasFailing = heartbeatOk === false;
      heartbeatOk = true;
      if (wasFailing) log("rendezvous recovered");
      printStatus();
    })
    .catch((err) => {
      heartbeatOk = false;
      log(`rendezvous registration failed: ${err.message}`, "error");
      printStatus();
    });
};

// ---------- character ownership: local archive + cloud sync (plan §13.13) ----------
// While this server runs, it is the authoritative copy of every character in
// its campaign (§13.13.1) — but that claim is only honest if it started from
// a confirmed-current copy (§13.13.3). `archive` is the in-memory, always-
// current view; each file under ARCHIVE_DIR is its durable backing (plain
// JSON, .tmp + rename() so a crash mid-write never leaves a torn file — the
// discipline plan §13.7 always intended for this tier, now load-bearing
// instead of a read-only convenience).
const ARCHIVE_ROOT = opt("archive-dir", path.join(os.homedir(), "fh-archive"));
const ARCHIVE_DIR = path.join(ARCHIVE_ROOT, CODE);
const archive = new Map(); // pseudo -> { pseudo, campaign, build, profile }
let archiveSynced = false; // has this boot's cloud pull actually succeeded?
// Count of character writes whose mirror to the cloud has failed since boot.
// Never resets on its own — once a mirror misses, every later mirror for that
// same character will also 409 (its local revision has moved past what the
// cloud saw), so this stays a running total rather than guessing recovery.
let unmirroredWrites = 0;

const archiveFile = (pseudo) => path.join(ARCHIVE_DIR, `${pseudo}.json`);

const writeArchiveFile = (pseudo, record) => {
  fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
  const file = archiveFile(pseudo);
  const tmp = `${file}.tmp`;
  // Synchronous and blocking, unlike the roll JSONL's fire-and-forget append
  // (plan §12.7, never fsynced because it is a by-product, not the record of
  // truth). This file IS the record of truth — the write must land before
  // the caller's HTTP response is allowed to say "ok".
  fs.writeFileSync(tmp, JSON.stringify(record));
  fs.renameSync(tmp, file);
};

const readArchiveFile = (pseudo) => {
  try {
    return JSON.parse(fs.readFileSync(archiveFile(pseudo), "utf8"));
  } catch {
    return null;
  }
};

const loadArchiveFromDisk = () => {
  archive.clear();
  if (!fs.existsSync(ARCHIVE_DIR)) return;
  for (const name of fs.readdirSync(ARCHIVE_DIR)) {
    if (!name.endsWith(".json") || name.endsWith(".json.tmp")) continue;
    const pseudo = name.slice(0, -5);
    const record = readArchiveFile(pseudo);
    if (record) archive.set(pseudo, record);
    else log(`archive file for "${pseudo}" is corrupt, skipping`, "warn");
  }
};

// Plan §13.13.3: "before it will call itself ready, it pulls the current
// party/profile records for its campaign from the cloud and adopts their
// revisions as its own starting point." Whatever loadArchiveFromDisk() found
// from a previous run is read first — not to seed anything, only so that if
// the cloud's answer turns out to be numerically *behind* it, that gets a
// loud warning instead of a silent overwrite (see the loop below). The
// cloud's answer always wins the boot regardless: that is what makes "owner"
// an honest claim here rather than a guess.
const syncArchiveFromCloud = async () => {
  const previous = new Map(archive);
  const party = await getJson(`${WORKER_ORIGIN}/party/${encodeURIComponent(CODE)}`);
  const members = Array.isArray(party?.builds) ? party.builds : [];
  const next = new Map();
  for (const entry of members) {
    const pseudo = cleanPseudo(entry?.pseudo);
    if (!pseudo) continue; // the Worker itself would never produce this; skip rather than abort the whole sync over one bad entry
    const build = await getJson(`${WORKER_ORIGIN}/party/${encodeURIComponent(CODE)}/${encodeURIComponent(pseudo)}`);
    const profileResp = await getJson(`${WORKER_ORIGIN}/profile/${encodeURIComponent(CODE)}/${encodeURIComponent(pseudo)}`);
    next.set(pseudo, { pseudo, campaign: CODE, build, profile: profileResp?.profile || null });
  }
  // Detect — log only, no auto-merge (plan §13.13.7) — a previous run's write
  // that this table server never confirmed the cloud received. Without this
  // check that write would be replaced by the cloud's older copy right now,
  // silently, which is exactly the failure plan §13.13 exists to prevent.
  for (const [pseudo, prevRecord] of previous) {
    const freshRecord = next.get(pseudo);
    const prevBuildRev = prevRecord?.build?.revision ?? -1;
    const freshBuildRev = freshRecord?.build?.revision ?? -1;
    const prevProfileRev = prevRecord?.profile?.revision ?? -1;
    const freshProfileRev = freshRecord?.profile?.revision ?? -1;
    if (prevBuildRev > freshBuildRev || prevProfileRev > freshProfileRev) {
      log(
        `"${pseudo}": the local archive from before this restart was ahead of the cloud ` +
          `(build rev ${prevBuildRev} vs ${freshBuildRev}, profile rev ${prevProfileRev} vs ${freshProfileRev}). ` +
          `A write from before this restart may never have reached the cloud. Continuing with the cloud's ` +
          `copy (plan §13.13.3) — check with this player before assuming nothing changed.`,
        "warn",
      );
    }
  }
  archive.clear();
  for (const [pseudo, record] of next) {
    archive.set(pseudo, record);
    writeArchiveFile(pseudo, record);
  }
};

// Same fire-and-forget discipline as mirror() above (plan §12.6): the local
// write already succeeded and IS the authoritative record while this table
// server runs (plan §13.13.1). A failed mirror — a network blip, or the
// cloud having moved for a reason this table server never saw — is surfaced
// (never silently swallowed, plan §2) via the terminal panel and /health,
// not retried or queued (plan §13.13.7 rules out an offline queue).
const mirrorCharacterWrite = (url, body) => {
  postJson(url, body).catch((err) => {
    unmirroredWrites += 1;
    log(`character mirror to cloud failed (${url}): ${err.message}`, "warn");
    printStatus();
  });
};

// ---------- Cloudflare Tunnel (plan §12.2) ----------
// An HTTPS page cannot fetch http:// — this tunnel is not an optimization,
// it is the only way a browser on github.io can reach this process at all.
const startTunnel = () => {
  log("starting Cloudflare Tunnel…");
  let proc;
  try {
    proc = spawn("cloudflared", ["tunnel", "--url", `http://127.0.0.1:${PORT}`], { stdio: ["ignore", "pipe", "pipe"] });
  } catch (err) {
    tunnelHealthy = false;
    log(`could not start cloudflared: ${err.message}. Install it: brew install cloudflared`, "error");
    printStatus();
    return;
  }
  const urlPattern = /https:\/\/[a-z0-9-]+\.trycloudflare\.com/;
  const onData = (chunk) => {
    const text = chunk.toString();
    const match = urlPattern.exec(text);
    if (match && !tunnelUrl) {
      tunnelUrl = match[0];
      tunnelHealthy = true;
      log(`tunnel up: ${tunnelUrl}`);
      registerTable();
      printStatus();
    }
  };
  proc.stdout.on("data", onData);
  proc.stderr.on("data", onData);
  proc.on("exit", (code) => {
    tunnelHealthy = false;
    log(`cloudflared exited (code ${code}) — players cannot reach this table until it restarts`, "error");
    printStatus();
  });
  proc.on("error", (err) => {
    tunnelHealthy = false;
    log(`cloudflared error: ${err.message}. Install it: brew install cloudflared`, "error");
    printStatus();
  });
  return proc;
};

// ---------- HTTP server ----------
const gmAuthorized = (req) => {
  const header = req.headers.authorization || "";
  const m = /^Bearer\s+(.+)$/i.exec(header.trim());
  if (!m || !GM_TOKEN) return false;
  return timingSafeEqual(m[1], GM_TOKEN);
};

const corsHeaders = (req) => {
  const origin = req.headers.origin || "";
  const headers = {
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Max-Age": "600",
    Vary: "Origin",
  };
  if (isAllowedOrigin(origin)) headers["Access-Control-Allow-Origin"] = origin;
  return headers;
};

const sendJson = (res, req, data, status = 200) => {
  const body = JSON.stringify(data);
  res.writeHead(status, { "Content-Type": "application/json", ...corsHeaders(req) });
  res.end(body);
};

const clientIp = (req) => req.headers["cf-connecting-ip"] || req.socket.remoteAddress || "unknown";
const rateLimited = makeRateLimiter();

const readBody = (req, maxBytes = MAX_BODY_BYTES) =>
  new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        reject(new Error("body too large"));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });

const server = http.createServer(async (req, res) => {
  let u;
  try {
    u = new URL(req.url, `http://127.0.0.1:${PORT}`);
  } catch {
    return sendJson(res, req, { error: "bad request" }, 400);
  }
  const parts = u.pathname.replace(/\/+$/, "").split("/").filter(Boolean);

  if (req.method === "OPTIONS") {
    res.writeHead(204, corsHeaders(req));
    return res.end();
  }

  // ---------- /health ----------
  if (parts.length === 1 && parts[0] === "health" && req.method === "GET") {
    return sendJson(res, req, {
      ok: true,
      code: CODE,
      startedAt,
      connected: wsClients.size + sseClients.size,
      ws: wsClients.size,
      sse: sseClients.size,
      events: buffer.length,
      tunnel: tunnelUrl,
      tunnelHealthy,
      rendezvous: heartbeatOk,
      mirroring: MIRROR,
      characters: archive.size,
      archiveSynced,
      unmirroredWrites,
    });
  }

  // ---------- /feed/:code[/stream] ----------
  if (parts[0] === "feed" && parts[1]) {
    const code = parts[1];
    // This server was started for exactly one campaign (plan §12.9) — it has
    // no builds KV to consult, and every other code is a flat 403.
    if (code !== CODE) return sendJson(res, req, { error: "unknown campaign code" }, 403);

    if (parts[2] === "stream" && parts.length === 3 && req.method === "GET") {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        ...corsHeaders(req),
      });
      res.write("retry: 3000\n\n");
      // Last-Event-ID beats ?since (plan §12.3) — EventSource sends it
      // automatically on reconnect. Replay may overlap what the client
      // already has; dedupe-by-id on the reader is what makes that safe.
      const lastId = req.headers["last-event-id"] || cleanText(u.searchParams.get("since") || "", 40);
      for (const row of buffer) {
        if (lastId && row.seq <= lastId) continue;
        res.write(`id: ${row.seq}\nevent: fh-event\ndata: ${JSON.stringify(row.event)}\n\n`);
      }
      sseClients.add(res);
      printStatus();
      const heartbeat = setInterval(() => {
        try {
          res.write(": hb\n\n");
        } catch {
          clearInterval(heartbeat);
          sseClients.delete(res);
        }
      }, 15_000);
      req.on("close", () => {
        clearInterval(heartbeat);
        sseClients.delete(res);
        printStatus();
      });
      return;
    }

    if (parts.length === 2 && req.method === "GET") {
      const since = cleanText(u.searchParams.get("since") || "", 40);
      const limit = clampInt(u.searchParams.get("limit"), 1, 200, 200);
      const { events, cursor } = sinceSlice(buffer, since, limit);
      return sendJson(res, req, { schemaVersion: 1, lookbackMs: 0, cursor, events });
    }

    if (parts.length === 2 && req.method === "POST") {
      if (rateLimited(clientIp(req))) return sendJson(res, req, { error: "too many feed posts — slow down" }, 429);
      let raw;
      try {
        raw = await readBody(req);
      } catch {
        return sendJson(res, req, { error: "body too large" }, 413);
      }
      let body;
      try {
        body = JSON.parse(raw);
      } catch {
        return sendJson(res, req, { error: "invalid JSON" }, 400);
      }
      let event = safeFeedEvent(body, CODE);
      if (!event) return sendJson(res, req, { error: "invalid feed event" }, 400);
      // Per-character portrait for the AboveVTT bridge (plan §13.13 archive is
      // already in memory here — no extra fetch). Never blocks or fails the
      // roll: an unlinked or not-yet-synced character simply has no avatar.
      const archived = archive.get(cleanPseudo(event.actor.pseudo));
      event = attachActorAvatar(event, archived?.profile?.snapshot?.avatarUrl);
      const seq = publish(event);
      return sendJson(res, req, { ok: true, seq, id: event.id });
    }

    // Clears the LIVE ring buffer only — the session's JSONL transcript is
    // left alone on purpose (plan §12.7: it is a free session log, not
    // something a mid-session "clear" should be able to destroy).
    if (parts.length === 2 && req.method === "DELETE") {
      if (!gmAuthorized(req)) return sendJson(res, req, { error: "unauthorized" }, 401);
      const cleared = buffer.length;
      buffer.length = 0;
      printStatus();
      return sendJson(res, req, { ok: true, cleared });
    }
  }

  // ---------- /party/:code[/:pseudo] (plan §13.13 — served from the local archive) ----------
  if (parts[0] === "party" && parts[1]) {
    if (parts[1] !== CODE) return sendJson(res, req, { error: "unknown campaign code" }, 403);

    if (parts.length === 2 && req.method === "GET") {
      const builds = [...archive.values()]
        .filter((record) => record.build)
        .map((record) => ({ pseudo: record.pseudo, updatedAt: record.build.updatedAt }));
      return sendJson(res, req, { builds });
    }

    if (parts.length === 3 && req.method === "GET") {
      const pseudo = cleanPseudo(parts[2]);
      const record = pseudo ? archive.get(pseudo) : null;
      if (!record?.build) return sendJson(res, req, { error: "not found" }, 404);
      return sendJson(res, req, record.build);
    }
  }

  // ---------- /profile/:code/:pseudo (plan §13.13) ----------
  if (parts[0] === "profile" && parts[1] && parts[2]) {
    if (parts[1] !== CODE) return sendJson(res, req, { error: "unknown campaign code" }, 403);
    const pseudo = cleanPseudo(parts[2]);
    if (!pseudo) return sendJson(res, req, { error: "invalid character" }, 400);
    const record = archive.get(pseudo);
    if (!record?.build) return sendJson(res, req, { error: "character not found" }, 404);

    if (parts.length === 3 && req.method === "GET") {
      return sendJson(res, req, { profile: record.profile || defaultCharacterProfile() });
    }

    if (parts.length === 3 && req.method === "POST") {
      let raw;
      try {
        raw = await readBody(req, MAX_CHARACTER_BODY_BYTES);
      } catch {
        return sendJson(res, req, { error: "body too large" }, 413);
      }
      let body;
      try {
        body = JSON.parse(raw);
      } catch {
        return sendJson(res, req, { error: "invalid JSON" }, 400);
      }
      const result = applyProfilePatch(record.profile || defaultCharacterProfile(), body);
      if (!result.ok) return sendJson(res, req, result.body, result.status);
      record.profile = result.profile;
      writeArchiveFile(pseudo, record);
      if (MIRROR) {
        mirrorCharacterWrite(`${WORKER_ORIGIN}/profile/${encodeURIComponent(CODE)}/${encodeURIComponent(pseudo)}`, body);
      }
      return sendJson(res, req, { ok: true, profile: result.profile });
    }
  }

  // ---------- POST /builds (plan §13.13 — campaign lives in the body, not the
  // URL, because that is the Worker's own shape for this one route) ----------
  if (parts.length === 1 && parts[0] === "builds" && req.method === "POST") {
    let raw;
    try {
      raw = await readBody(req, MAX_CHARACTER_BODY_BYTES);
    } catch {
      return sendJson(res, req, { error: "body too large" }, 413);
    }
    let body;
    try {
      body = JSON.parse(raw);
    } catch {
      return sendJson(res, req, { error: "invalid JSON" }, 400);
    }
    const pseudo = cleanPseudo(body?.pseudo);
    if (!pseudo || !body?.build) return sendJson(res, req, { error: "pseudo and build are required" }, 400);
    // Same rule as every other route above: this table server was started
    // for exactly one campaign, and every other code is a flat 403.
    if (body.campaign !== CODE) return sendJson(res, req, { error: "unknown campaign code" }, 403);
    const existing = archive.get(pseudo);
    const result = applyBuildWrite(existing?.build || null, pseudo, body);
    if (!result.ok) return sendJson(res, req, result.body, result.status);
    const record = { pseudo, campaign: CODE, build: result.record, profile: existing?.profile || null };
    archive.set(pseudo, record);
    writeArchiveFile(pseudo, record);
    if (MIRROR) mirrorCharacterWrite(`${WORKER_ORIGIN}/builds`, body);
    return sendJson(res, req, {
      ok: true,
      pseudo,
      campaign: CODE,
      updatedAt: result.record.updatedAt,
      revision: result.record.revision,
    });
  }

  return sendJson(res, req, { error: "not found" }, 404);
});

// ---------- WebSocket upgrade: GET /feed/:code/ws[?since=SEQ] ----------
// The production delivery path. `since` is the same cursor the poll route uses
// (plan §12.3) — a reconnecting dock replays from where it stopped, and the
// replay may overlap what it already holds because dedupe-by-id makes that
// safe. What SSE gave for free via Last-Event-ID is here an explicit query
// param, which is the real cost of the fallback and it is a small one.
server.on("upgrade", (req, socket) => {
  let u;
  try {
    u = new URL(req.url, `http://127.0.0.1:${PORT}`);
  } catch {
    socket.destroy();
    return;
  }
  const parts = u.pathname.replace(/\/+$/, "").split("/").filter(Boolean);
  if (!(parts[0] === "feed" && parts[1] && parts[2] === "ws" && parts.length === 3)) {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    socket.destroy();
    return;
  }
  if (parts[1] !== CODE) {
    socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
    socket.destroy();
    return;
  }

  const shake = handshakeResponse(req, isAllowedOrigin);
  if (!shake.ok) {
    socket.write(`${shake.status}\r\n\r\n`);
    socket.destroy();
    return;
  }
  socket.write(shake.headers);
  socket.setNoDelay(true);

  const since = cleanText(u.searchParams.get("since") || "", 40);
  for (const row of buffer) {
    if (since && row.seq <= since) continue;
    socket.write(encodeFrame(JSON.stringify({ seq: row.seq, event: row.event })));
  }

  wsClients.add(socket);
  printStatus();

  const drop = () => {
    if (!wsClients.has(socket)) return;
    clearInterval(ping);
    wsClients.delete(socket);
    socket.destroy();
    printStatus();
  };
  const ping = setInterval(() => {
    try {
      socket.write(encodeFrame("", OPCODE.ping));
    } catch {
      drop();
    }
  }, 20_000);

  let inbox = Buffer.alloc(0);
  socket.on("data", (chunk) => {
    inbox = Buffer.concat([inbox, chunk]);
    const { frames, rest, overflow } = decodeFrames(inbox);
    if (overflow) return drop();
    inbox = rest;
    for (const f of frames) {
      if (f.opcode === OPCODE.close) {
        try {
          socket.write(encodeFrame("", OPCODE.close));
        } catch {
          /* peer already gone */
        }
        return drop();
      }
      if (f.opcode === OPCODE.ping) {
        try {
          socket.write(encodeFrame(f.payload, OPCODE.pong));
        } catch {
          return drop();
        }
      }
      // A dock has nothing to say on this socket — rolls go up over POST, which
      // keeps the per-roll ack that drives the offline state (plan §12.3).
      // Text frames are therefore ignored rather than parsed.
    }
  });
  socket.on("close", drop);
  socket.on("error", drop);
});

server.on("error", (err) => {
  if (err.code === "EADDRINUSE") {
    log(`port ${PORT} is already in use — another table server running? Try --port`, "error");
  } else {
    log(`server error: ${err.message}`, "error");
  }
  process.exit(1);
});

// Bind to loopback ONLY (plan §12.9) — the tunnel is then the sole way in,
// and a dead tunnel means genuinely closed, not quietly exposed to the wifi.
const startServer = () => {
  server.listen(PORT, "127.0.0.1", () => {
    log(`listening on http://127.0.0.1:${PORT} (loopback only)`);
    printStatus();
    if (TUNNEL) {
      startTunnel();
    } else {
      log("--no-tunnel: nobody outside this machine can reach the feed until you provide your own tunnel", "warn");
    }
    setInterval(printStatus, 30_000).unref();
    // Registration is how a dock finds this table at all (plan §12.4) — it is
    // NOT the same thing as mirroring events to the cloud backstop (§12.6), and
    // must keep heartbeating even when --no-mirror disables the latter.
    setInterval(registerTable, 5 * 60 * 1000).unref();
  });
};

// Plan §13.13.3: this table server claims to OWN its campaign's characters —
// a claim that is only true the instant it starts if it is actually checked
// first. loadArchiveFromDisk() always runs, so syncArchiveFromCloud() has a
// previous local state to compare against; the cloud's answer then either
// confirms or replaces it (see that function for what "replaces" logs when
// the previous local copy was ahead). If the cloud can't be reached or
// answers with something unusable, this process does not actually know the
// party's true state — so, deliberately, it refuses to bind its port at all
// rather than start silently unsynced. This is the one place plan §13.13
// leaves as a genuinely open design question ("what if the initial pull
// fails"); "refuse to start" is the answer given here, so a DM sees a clear
// red error before a game rather than a table server quietly serving a stale
// or empty archive. --no-mirror is the explicit, loud opt-out for local dev
// without a Worker to talk to.
loadArchiveFromDisk();
if (MIRROR) {
  syncArchiveFromCloud()
    .then(() => {
      archiveSynced = true;
      log(`character archive synced from the cloud (${archive.size} character(s))`);
      startServer();
    })
    .catch((err) => {
      log(`could not sync the character archive from the cloud: ${err.message}`, "error");
      log(
        `refusing to start — this table server cannot claim to own campaign "${CODE}"'s characters ` +
          `without a confirmed-current copy (plan §13.13.3). Check the network and the Worker ` +
          `(${WORKER_ORIGIN}), then retry. Use --no-mirror to run detached from the cloud instead.`,
        "error",
      );
      process.exit(1);
    });
} else {
  log("--no-mirror: skipping the cloud sync — starting from whatever is already on disk (NOT guaranteed up to date)", "warn");
  startServer();
}

const shutdown = (signal) => {
  log(`${signal} received, shutting down…`);
  if (GM_TOKEN) deleteTable();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1000).unref();
};
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
