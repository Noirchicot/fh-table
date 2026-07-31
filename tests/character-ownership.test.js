// Integration tests for plan §13.13 (character ownership). Unlike lib.test.js
// and ws.test.js, these run the real table-server.mjs as a child process —
// exactly how a DM runs it — pointed at a throwaway archive directory and, for
// the cloud-facing scenarios, a fake Worker (a plain node:http server standing
// in for fh-worker's /party, /profile and /builds contract). Nothing inside
// table-server.mjs is mocked.

import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SERVER_PATH = path.join(__dirname, "..", "table-server.mjs");
const REPO_ROOT = path.join(__dirname, "..");
const CODE = "TESTFH";

// ---------- a fake Worker: just enough of fh-worker's contract to drive the
// table server's boot sync and mirror calls, including the revision/409 rule
// (plan §13.13.2) so a mirrored write can be seen to succeed or conflict. ----------
const startFakeWorker = () =>
  new Promise((resolve) => {
    const builds = new Map(); // pseudo -> {pseudo, campaign, updatedAt, revision, build}
    const profiles = new Map(); // pseudo -> profile (public shape)
    const requests = []; // {method, pathname, body} — for assertions

    const defaultProfile = () => ({
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
      updatedAt: new Date().toISOString(),
    });

    const server = http.createServer((req, res) => {
      let raw = "";
      req.on("data", (c) => (raw += c));
      req.on("end", () => {
        let body = null;
        try {
          body = raw ? JSON.parse(raw) : null;
        } catch {
          /* leave body null — no fake-worker route needs to reject bad JSON */
        }
        const u = new URL(req.url, "http://127.0.0.1");
        const parts = u.pathname.split("/").filter(Boolean);
        requests.push({ method: req.method, pathname: u.pathname, body });

        const send = (status, data) => {
          res.writeHead(status, { "Content-Type": "application/json" });
          res.end(JSON.stringify(data));
        };

        if (req.method === "GET" && parts[0] === "party" && parts.length === 2) {
          return send(200, { builds: [...builds.values()].map((b) => ({ pseudo: b.pseudo, updatedAt: b.updatedAt })) });
        }
        if (req.method === "GET" && parts[0] === "party" && parts.length === 3) {
          const b = builds.get(decodeURIComponent(parts[2]));
          return b ? send(200, b) : send(404, { error: "not found" });
        }
        if (req.method === "GET" && parts[0] === "profile" && parts.length === 3) {
          const p = profiles.get(decodeURIComponent(parts[2])) || defaultProfile();
          return send(200, { profile: p });
        }
        if (req.method === "POST" && parts[0] === "builds" && parts.length === 1) {
          const existing = builds.get(body.pseudo) || null;
          const currentRevision = existing?.revision || 0;
          const supplied = Number.isFinite(body.revision) ? body.revision : 0;
          if (supplied !== currentRevision) return send(409, { error: "conflict", currentRevision, current: existing });
          const record = {
            pseudo: body.pseudo,
            campaign: body.campaign,
            updatedAt: new Date().toISOString(),
            revision: currentRevision + 1,
            build: body.build,
          };
          builds.set(body.pseudo, record);
          return send(200, { ok: true, pseudo: record.pseudo, campaign: record.campaign, updatedAt: record.updatedAt, revision: record.revision });
        }
        if (req.method === "POST" && parts[0] === "profile" && parts.length === 3) {
          const pseudo = decodeURIComponent(parts[2]);
          const current = profiles.get(pseudo) || defaultProfile();
          const supplied = Number.isFinite(body.revision) ? body.revision : 0;
          if (supplied !== current.revision) return send(409, { error: "conflict", currentRevision: current.revision, current });
          const next = { ...current, ...body, revision: current.revision + 1, updatedAt: new Date().toISOString() };
          delete next.revision;
          next.revision = current.revision + 1;
          profiles.set(pseudo, next);
          return send(200, { ok: true, profile: next });
        }
        return send(404, { error: "not found" });
      });
    });

    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      resolve({
        server,
        origin: `http://127.0.0.1:${port}`,
        builds,
        profiles,
        requests,
        close: () => new Promise((r) => server.close(r)),
        // Directly seed a build/profile as if a player had created it before
        // this table server ever started — used by the initial-pull test.
        seedBuild: (pseudo, build, revision = 1) =>
          builds.set(pseudo, { pseudo, campaign: CODE, updatedAt: new Date().toISOString(), revision, build }),
      });
    });
  });

// A port that was briefly bound and immediately released: near-guaranteed to
// refuse connections for the short window this test needs, without depending
// on any specific unused port number on the test machine.
const unreachableOrigin = () =>
  new Promise((resolve) => {
    const probe = http.createServer();
    probe.listen(0, "127.0.0.1", () => {
      const { port } = probe.address();
      probe.close(() => resolve(`http://127.0.0.1:${port}`));
    });
  });

const waitForHealth = async (port, { tries = 100, intervalMs = 50 } = {}) => {
  for (let i = 0; i < tries; i += 1) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/health`);
      if (res.ok) return res.json();
    } catch {
      /* not up yet */
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error("table server never became healthy");
};

const waitForExit = (child) => new Promise((resolve) => child.on("exit", (code) => resolve(code)));

let nextPort = 18901;
const spawnTable = (workerOrigin, extraArgs = []) => {
  const port = nextPort;
  nextPort += 1;
  const archiveDir = fs.mkdtempSync(path.join(os.tmpdir(), "fh-archive-test-"));
  const stdout = [];
  const stderr = [];
  const child = spawn(
    process.execPath,
    [
      SERVER_PATH,
      CODE,
      "--port",
      String(port),
      "--no-tunnel",
      "--archive-dir",
      archiveDir,
      "--worker-origin",
      workerOrigin,
      ...extraArgs,
    ],
    { cwd: REPO_ROOT, stdio: ["ignore", "pipe", "pipe"] },
  );
  child.stdout.on("data", (c) => stdout.push(c.toString()));
  child.stderr.on("data", (c) => stderr.push(c.toString()));
  return { child, port, archiveDir, stdout, stderr };
};

const stopTable = async ({ child, archiveDir }) => {
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGTERM");
    await waitForExit(child);
  }
  fs.rmSync(archiveDir, { recursive: true, force: true });
};

// ---------- 1. initial pull from the cloud at startup ----------
test("a table server starting up pulls the existing party from the cloud before it will answer requests", async () => {
  const worker = await startFakeWorker();
  worker.seedBuild("Nodren", { level: 2, class: "Bard" }, 3);

  const table = spawnTable(worker.origin);
  try {
    const health = await waitForHealth(table.port);
    assert.equal(health.archiveSynced, true);
    assert.equal(health.characters, 1);

    const party = await (await fetch(`http://127.0.0.1:${table.port}/party/${CODE}`)).json();
    assert.deepEqual(party.builds.map((b) => b.pseudo), ["Nodren"]);

    const build = await (await fetch(`http://127.0.0.1:${table.port}/party/${CODE}/Nodren`)).json();
    assert.equal(build.revision, 3);
    assert.deepEqual(build.build, { level: 2, class: "Bard" });

    // The profile pull ran too, even though the Worker had never stored one.
    const profile = await (await fetch(`http://127.0.0.1:${table.port}/profile/${CODE}/Nodren`)).json();
    assert.equal(profile.profile.revision, 0);

    // The pull must actually have landed on disk, not just in memory.
    const archived = JSON.parse(fs.readFileSync(path.join(table.archiveDir, CODE, "Nodren.json"), "utf8"));
    assert.equal(archived.build.revision, 3);
  } finally {
    await stopTable(table);
    await worker.close();
  }
});

// ---------- 2. a normal write ----------
test("POST /builds accepts a well-formed write and stores it locally", async () => {
  const table = spawnTable("http://127.0.0.1:1", ["--no-mirror"]); // no cloud needed for this one
  try {
    const health = await waitForHealth(table.port);
    assert.equal(health.characters, 0);

    const res = await fetch(`http://127.0.0.1:${table.port}/builds`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pseudo: "Yedrivel", campaign: CODE, revision: 0, build: { level: 1 } }),
    });
    assert.equal(res.status, 200);
    const json = await res.json();
    assert.equal(json.ok, true);
    assert.equal(json.revision, 1);

    const fetched = await (await fetch(`http://127.0.0.1:${table.port}/party/${CODE}/Yedrivel`)).json();
    assert.equal(fetched.revision, 1);
    assert.deepEqual(fetched.build, { level: 1 });
  } finally {
    await stopTable(table);
  }
});

test("a table server never accepts a write for a campaign other than the one it was launched with", async () => {
  const table = spawnTable("http://127.0.0.1:1", ["--no-mirror"]);
  try {
    await waitForHealth(table.port);
    const res = await fetch(`http://127.0.0.1:${table.port}/builds`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pseudo: "Yedrivel", campaign: "SOME-OTHER-CODE", revision: 0, build: { level: 1 } }),
    });
    assert.equal(res.status, 403);
  } finally {
    await stopTable(table);
  }
});

// ---------- 3. a conflict, detected and refused ----------
test("a stale revision on POST /builds is refused with 409 and the current record, not applied", async () => {
  const table = spawnTable("http://127.0.0.1:1", ["--no-mirror"]);
  try {
    await waitForHealth(table.port);
    const create = async (revision, build) =>
      fetch(`http://127.0.0.1:${table.port}/builds`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pseudo: "Kala", campaign: CODE, revision, build }),
      });

    assert.equal((await create(0, { level: 1 })).status, 200);

    // Retrying with the same (now stale) revision must be rejected, not merged.
    const conflict = await create(0, { level: 99 });
    assert.equal(conflict.status, 409);
    const conflictBody = await conflict.json();
    assert.equal(conflictBody.error, "conflict");
    assert.equal(conflictBody.currentRevision, 1);
    assert.equal(conflictBody.current.build.level, 1);

    // The rejected write must not have landed.
    const fetched = await (await fetch(`http://127.0.0.1:${table.port}/party/${CODE}/Kala`)).json();
    assert.equal(fetched.revision, 1);
    assert.equal(fetched.build.level, 1);

    // The correct next revision still goes through.
    assert.equal((await create(1, { level: 2 })).status, 200);
  } finally {
    await stopTable(table);
  }
});

test("a stale revision on POST /profile/:code/:pseudo is refused with 409, not applied", async () => {
  const table = spawnTable("http://127.0.0.1:1", ["--no-mirror"]);
  try {
    await waitForHealth(table.port);
    await fetch(`http://127.0.0.1:${table.port}/builds`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pseudo: "Kala", campaign: CODE, revision: 0, build: { level: 1 } }),
    });

    const patch = async (revision, rollPrefs) =>
      fetch(`http://127.0.0.1:${table.port}/profile/${CODE}/Kala`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ revision, rollPrefs }),
      });

    assert.equal((await patch(0, { sound: "on" })).status, 200);
    const conflict = await patch(0, { sound: "off" });
    assert.equal(conflict.status, 409);
    const conflictBody = await conflict.json();
    assert.equal(conflictBody.currentRevision, 1);
    assert.deepEqual(conflictBody.current.rollPrefs, { sound: "on" });
  } finally {
    await stopTable(table);
  }
});

// ---------- 4. mirroring to the cloud after a successful local write ----------
test("a successful local write is mirrored to the cloud immediately, with the same revision", async () => {
  const worker = await startFakeWorker();
  const table = spawnTable(worker.origin);
  try {
    await waitForHealth(table.port);

    const res = await fetch(`http://127.0.0.1:${table.port}/builds`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pseudo: "Sol", campaign: CODE, revision: 0, build: { level: 1 } }),
    });
    assert.equal(res.status, 200);

    // The mirror is fire-and-forget (plan §12.6's pattern) — give it a moment.
    for (let i = 0; i < 50 && !worker.builds.has("Sol"); i += 1) await new Promise((r) => setTimeout(r, 20));
    assert.ok(worker.builds.has("Sol"), "the cloud never received the mirrored build");
    assert.equal(worker.builds.get("Sol").revision, 1);

    const profileRes = await fetch(`http://127.0.0.1:${table.port}/profile/${CODE}/Sol`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ revision: 0, rollPrefs: { sound: "on" } }),
    });
    assert.equal(profileRes.status, 200);
    for (let i = 0; i < 50 && !worker.profiles.has("Sol"); i += 1) await new Promise((r) => setTimeout(r, 20));
    assert.ok(worker.profiles.has("Sol"), "the cloud never received the mirrored profile patch");
    assert.equal(worker.profiles.get("Sol").revision, 1);

    const health = await (await fetch(`http://127.0.0.1:${table.port}/health`)).json();
    assert.equal(health.unmirroredWrites, 0);
  } finally {
    await stopTable(table);
    await worker.close();
  }
});

// ---------- 5. refusal to start when the initial pull fails ----------
test("the table server refuses to bind its port if the initial cloud sync fails", async () => {
  const badOrigin = await unreachableOrigin();
  const table = spawnTable(badOrigin);
  try {
    const exitCode = await waitForExit(table.child);
    assert.notEqual(exitCode, 0);
    const combined = [...table.stdout, ...table.stderr].join("");
    assert.match(combined, /refusing to start/i);

    // And, separately: it must never have opened its HTTP port at all.
    await assert.rejects(fetch(`http://127.0.0.1:${table.port}/health`));
  } finally {
    fs.rmSync(table.archiveDir, { recursive: true, force: true });
  }
});
