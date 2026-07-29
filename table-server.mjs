#!/usr/bin/env node
// Fate's Hand — table server (plan §12 / package 12a).
//
// Run on the DM's own machine during a session. Hosts the LIVE campaign feed
// for one campaign at a time: an SSE stream for delivery, the same GET/POST
// contract the cloud Worker exposes at /feed, mirrored onward to that Worker
// as a backstop. See ~/tools/fh-phb/COMPANION-BUILD-PLAN.md §12 for the
// design and the "why" behind every decision below — this file implements
// it, it does not decide it.
//
// Everything above the transport is unchanged from plan §11: settlement at
// openRollState, rollId+rev revisions, dedupe by id, the fh-event/1 and
// fh-roll/1 shapes. This process only changes where events are stored and
// how a dock reaches them.
//
// Usage:
//   node table-server.mjs <CAMPAIGN-CODE> [--port 8791] [--no-mirror] [--no-tunnel]
//
// Zero npm dependencies, on purpose (§12.8): this runs moments before a game,
// players waiting, and must never fail on an install.

import http from "node:http";
import https from "node:https";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs";
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
} from "./lib.mjs";
import { encodeFrame, decodeFrames, handshakeResponse, OPCODE } from "./ws.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------- config ----------
const args = process.argv.slice(2);
const CODE = args.find((a) => !a.startsWith("--"));
if (!CODE) {
  console.error("usage: node table-server.mjs <CAMPAIGN-CODE> [--port 8791] [--no-mirror] [--no-tunnel]");
  process.exit(1);
}
const flag = (name) => args.includes(`--${name}`);
const opt = (name, fallback) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};
const PORT = Number(opt("port", "8791"));
const MIRROR = !flag("no-mirror");
const TUNNEL = !flag("no-tunnel");
const WORKER_ORIGIN = "https://fh-builds.noirchicot.workers.dev";
const DATA_DIR = path.join(__dirname, "data");
const RING_MAX = 400; // plan §12.7 — generous for one session
const MAX_BODY_BYTES = 8_000; // plan §12.9, matches the Worker's safeOpaque caps

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

// ---------- outbound HTTPS helpers (to the cloud Worker) ----------
const postJson = (url, body, headers = {}) =>
  new Promise((resolve, reject) => {
    const data = Buffer.from(JSON.stringify(body));
    const u = new URL(url);
    const req = https.request(
      {
        hostname: u.hostname,
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

const deleteTable = () => {
  const u = new URL(`${WORKER_ORIGIN}/table/${encodeURIComponent(CODE)}`);
  const req = https.request({
    hostname: u.hostname,
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

const readBody = (req) =>
  new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
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
      const event = safeFeedEvent(body, CODE);
      if (!event) return sendJson(res, req, { error: "invalid feed event" }, 400);
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

const shutdown = (signal) => {
  log(`${signal} received, shutting down…`);
  if (GM_TOKEN) deleteTable();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1000).unref();
};
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
