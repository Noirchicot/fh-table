// Unit tests for the pure logic in lib.mjs — no server, no tunnel, no
// network. Run with `node --test tests/*.test.js`, same convention as
// ~/tools/fh-worker/tests.

import test from "node:test";
import assert from "node:assert/strict";
import {
  safeFeedEvent,
  makeSeqGenerator,
  sinceSlice,
  isAllowedOrigin,
  timingSafeEqual,
  makeRateLimiter,
} from "../lib.mjs";

test("a valid roll survives with its actor and display intact", () => {
  const event = safeFeedEvent(
    { type: "roll", actor: { pseudo: "Sol", character: "Yedrivel" }, display: { title: "Hunting", total: 18 } },
    "FH2",
  );
  assert.equal(event.type, "roll");
  assert.equal(event.actor.pseudo, "Sol");
  assert.equal(event.actor.character, "Yedrivel");
  assert.equal(event.campaign, "FH2");
  assert.deepEqual(event.display, { title: "Hunting", total: 18 });
});

test("a roll without a display is rejected, a note without one is not", () => {
  assert.equal(safeFeedEvent({ type: "roll", actor: { pseudo: "Sol" } }, "FH2"), null);
  const note = safeFeedEvent({ type: "note", actor: { pseudo: "DM" } }, "FH2");
  assert.equal(note.type, "note");
  assert.equal(note.display, null);
});

test("an event with no type or no pseudo is rejected", () => {
  assert.equal(safeFeedEvent({ actor: { pseudo: "Sol" }, display: {} }, "FH2"), null);
  assert.equal(safeFeedEvent({ type: "roll", actor: {}, display: {} }, "FH2"), null);
});

test("the campaign comes from the argument, never from the body", () => {
  const event = safeFeedEvent({ type: "note", actor: { pseudo: "DM" }, campaign: "FH9" }, "FH2");
  assert.equal(event.campaign, "FH2");
});

test("revisions carry rollId and a clamped rev", () => {
  const event = safeFeedEvent(
    { type: "roll", actor: { pseudo: "Sol" }, display: {}, rollId: "abc123", rev: 5000 },
    "FH2",
  );
  assert.equal(event.rollId, "abc123");
  assert.equal(event.rev, 999); // clamped to the 0..999 range
});

test("the three designed intent kinds are all accepted", () => {
  for (const kind of ["check", "damage", "spell"]) {
    const event = safeFeedEvent({ type: "roll", actor: { pseudo: "Sol" }, display: {}, intent: { kind } }, "FH2");
    assert.equal(event.intent.kind, kind);
  }
});

test("an unknown intent kind is stripped without losing the display", () => {
  const event = safeFeedEvent(
    { type: "roll", actor: { pseudo: "Sol" }, display: { title: "x" }, intent: { kind: "teleport" } },
    "FH2",
  );
  assert.equal(event.intent, null);
  assert.deepEqual(event.display, { title: "x" });
});

test("an oversized display is refused rather than truncated", () => {
  const huge = { blob: "x".repeat(10_000) };
  assert.equal(safeFeedEvent({ type: "roll", actor: { pseudo: "Sol" }, display: huge }, "FH2"), null);
});

test("the sequence sorts lexicographically in time order", () => {
  let now = 1_000_000_000_000;
  const seq = makeSeqGenerator(() => now);
  const a = seq();
  now += 5;
  const b = seq();
  assert.ok(a < b);
});

test("two calls at the same instant still get distinct, ordered keys", () => {
  const now = 1_000_000_000_000;
  const seq = makeSeqGenerator(() => now);
  const a = seq();
  const b = seq();
  assert.notEqual(a, b);
  assert.ok(a < b); // the monotonic guard bumps ms by 1 rather than colliding
});

test("the clock going backwards never produces a seq that sorts earlier", () => {
  let now = 2_000_000_000_000;
  const seq = makeSeqGenerator(() => now);
  const a = seq();
  now -= 10_000; // a system clock adjustment
  const b = seq();
  assert.ok(b > a);
});

test("sinceSlice returns only events after the cursor, most recent first respected", () => {
  const buffer = [
    { seq: "0000000000001-aaaa", event: { id: "1" } },
    { seq: "0000000000002-bbbb", event: { id: "2" } },
    { seq: "0000000000003-cccc", event: { id: "3" } },
  ];
  const { events, cursor } = sinceSlice(buffer, "0000000000001-aaaa", 200);
  assert.deepEqual(events.map((e) => e.id), ["2", "3"]);
  assert.equal(cursor, "0000000000003-cccc");
});

test("sinceSlice with no since returns everything and caps at limit", () => {
  const buffer = Array.from({ length: 5 }, (_, i) => ({ seq: `seq${i}`, event: { id: i } }));
  const { events, cursor } = sinceSlice(buffer, "", 3);
  assert.deepEqual(events.map((e) => e.id), [2, 3, 4]);
  assert.equal(cursor, "seq4");
});

test("sinceSlice with nothing new echoes the since cursor back", () => {
  const buffer = [{ seq: "0000000000001-aaaa", event: { id: "1" } }];
  const { events, cursor } = sinceSlice(buffer, "0000000000005-zzzz", 200);
  assert.deepEqual(events, []);
  assert.equal(cursor, "0000000000005-zzzz");
});

test("the dock's github.io origin is allowed, an unrelated origin is not", () => {
  assert.ok(isAllowedOrigin("https://noirchicot.github.io"));
  assert.ok(isAllowedOrigin("http://localhost:8080"));
  assert.ok(isAllowedOrigin("http://127.0.0.1:8131"));
  assert.equal(isAllowedOrigin("https://evil.example"), false);
  assert.equal(isAllowedOrigin(""), false);
});

test("timingSafeEqual matches equal tokens and rejects unequal ones", () => {
  assert.ok(timingSafeEqual("same-token", "same-token"));
  assert.equal(timingSafeEqual("same-token", "different"), false);
});

test("the rate limiter allows up to the limit and then blocks", () => {
  let now = 0;
  const limited = makeRateLimiter(3, 60_000, () => now);
  assert.equal(limited("1.2.3.4"), false);
  assert.equal(limited("1.2.3.4"), false);
  assert.equal(limited("1.2.3.4"), false);
  assert.equal(limited("1.2.3.4"), true); // 4th call in the same window
});

test("the rate limiter tracks IPs independently", () => {
  let now = 0;
  const limited = makeRateLimiter(1, 60_000, () => now);
  assert.equal(limited("1.2.3.4"), false);
  assert.equal(limited("5.6.7.8"), false); // a different IP has its own budget
});

test("the rate limiter resets in a new window", () => {
  let now = 0;
  const limited = makeRateLimiter(1, 60_000, () => now);
  assert.equal(limited("1.2.3.4"), false);
  assert.equal(limited("1.2.3.4"), true);
  now = 61_000;
  assert.equal(limited("1.2.3.4"), false); // new window, fresh budget
});
