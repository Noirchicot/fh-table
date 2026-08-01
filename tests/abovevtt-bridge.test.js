import test from "node:test";
import assert from "node:assert/strict";
import {
  chatHtml,
  chooseAboveVttTab,
  cleanCampaignCode,
  emptyLedger,
  escapeHtml,
  normalizeLedger,
  planEvent,
  KEEPALIVE_MS,
  startWebSocketKeepAlive,
} from "../extension/bridge-core.mjs";

const event = (overrides = {}) => ({
  schema: "fh-event/1",
  id: "event-1",
  type: "roll",
  rollId: "roll-1",
  rev: 0,
  actor: { pseudo: "Sol", character: "Yedrivel" },
  display: { schema: "fh-roll/1", title: "Hunting", total: 27, parts: [], badges: [] },
  intent: { kind: "check", check: "Hunting", ability: "WIS", total: 27, natural: 20, dc: 15, outcome: "critical-success" },
  ...overrides,
});

test("a check becomes one resolved plain-text AboveVTT line", () => {
  const plan = planEvent(emptyLedger(), event());
  assert.equal(plan.action, "deliver");
  assert.equal(plan.revised, false);
  assert.match(plan.message, /Yedrivel — Hunting \(WIS\): 27/);
  assert.match(plan.message, /d20 20/);
  assert.match(plan.message, /CRITICAL SUCCESS/);
  assert.doesNotMatch(plan.message, /1d20\+|\/hit|\/save/);
});

test("the same event id is never delivered twice", () => {
  const first = planEvent(emptyLedger(), event());
  const duplicate = planEvent(first.nextLedger, event());
  assert.equal(duplicate.action, "ignore");
  assert.equal(duplicate.reason, "duplicate-id");
});

test("a higher revision supersedes the prior total instead of looking like a new roll", () => {
  const first = planEvent(emptyLedger(), event());
  const revised = planEvent(first.nextLedger, event({ id: "event-2", rev: 1, display: { title: "Hunting", total: 31 }, intent: { kind: "check", check: "Hunting", ability: "WIS", total: 31, natural: 20, dc: 15, outcome: "critical-success" } }));
  assert.equal(revised.action, "deliver");
  assert.equal(revised.revised, true);
  assert.match(revised.message, /^UPDATE r1/);
  assert.match(revised.message, /supersedes 27/);
});

test("an older out-of-order revision is ignored", () => {
  const current = planEvent(emptyLedger(), event({ id: "event-2", rev: 2 }));
  const stale = planEvent(current.nextLedger, event({ id: "event-3", rev: 1 }));
  assert.equal(stale.action, "ignore");
  assert.equal(stale.reason, "stale-revision");
});

test("unknown future intent kinds degrade to a printed display line", () => {
  const plan = planEvent(emptyLedger(), event({ intent: { kind: "teleport" } }));
  assert.equal(plan.action, "deliver");
  assert.match(plan.message, /teleport intent: printed only/);
});

test("untrusted display text is escaped before it reaches inject_chat", () => {
  assert.equal(escapeHtml('<img src=x onerror="boom">'), "&lt;img src=x onerror=&quot;boom&quot;&gt;");
  const html = chatHtml("Yedrivel <script>alert(1)</script>");
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /&lt;script&gt;/);
});

test("malformed events are refused instead of acknowledged as delivered", () => {
  const plan = planEvent(emptyLedger(), { id: "x" });
  assert.equal(plan.action, "reject");
});

test("campaign codes are normalized and bounded", () => {
  assert.equal(cleanCampaignCode(" fh2 "), "FH2");
  assert.equal(cleanCampaignCode("../../fh2"), "");
});

test("only one connected DM table is selected", () => {
  assert.deepEqual(chooseAboveVttTab([{ tabId: 7, ready: true, connected: true, dm: true }]), { ok: true, tabId: 7 });
  assert.equal(chooseAboveVttTab([{ tabId: 7, ready: true, connected: false, dm: true }]).ok, false);
  assert.deepEqual(
    chooseAboveVttTab([
      { tabId: 7, ready: true, connected: true, dm: true, active: false },
      { tabId: 8, ready: true, connected: true, dm: true, active: true },
    ]),
    { ok: true, tabId: 8 },
  );
});

test("stored ledger state is bounded and normalized", () => {
  const ledger = normalizeLedger({ seenIds: Array.from({ length: 1000 }, (_, i) => `e${i}`), rolls: { r: { rev: "2", total: "19", title: "Hunting" } } });
  assert.equal(ledger.seenIds.length, 800);
  assert.deepEqual(ledger.rolls.r, { rev: 2, total: 19, title: "Hunting", eventId: "" });
});

test("the extension keeps its Manifest V3 worker alive with an application WebSocket message", () => {
  const sent = [];
  let tick;
  let cleared = null;
  const socket = { readyState: 1, send: (message) => sent.push(message) };
  const stop = startWebSocketKeepAlive(() => socket, {
    setIntervalFn(callback, ms) {
      assert.equal(ms, KEEPALIVE_MS);
      tick = callback;
      return 17;
    },
    clearIntervalFn(id) {
      cleared = id;
    },
  });

  tick();
  assert.deepEqual(sent, ["keepalive"]);
  socket.readyState = 3;
  tick();
  assert.deepEqual(sent, ["keepalive"]);
  stop();
  assert.equal(cleared, 17);
});
