// Frame-level tests for the hand-rolled WebSocket in ws.mjs. These matter more
// than usual: there is no library underneath to be right on our behalf.

import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { isAllowedOrigin } from "../lib.mjs";
import { acceptKey, encodeFrame, decodeFrames, handshakeResponse, OPCODE, WS_GUID } from "../ws.mjs";

// Masks a payload the way a browser client does, so decodeFrames is tested
// against real client-shaped input rather than the frames we emit.
const clientFrame = (text, opcode = OPCODE.text) => {
  const payload = Buffer.from(text, "utf8");
  const mask = crypto.randomBytes(4);
  const masked = Buffer.from(payload);
  for (let i = 0; i < masked.length; i += 1) masked[i] ^= mask[i % 4];
  let head;
  if (payload.length < 126) {
    head = Buffer.from([0x80 | opcode, 0x80 | payload.length]);
  } else {
    head = Buffer.alloc(4);
    head[0] = 0x80 | opcode;
    head[1] = 0x80 | 126;
    head.writeUInt16BE(payload.length, 2);
  }
  return Buffer.concat([head, mask, masked]);
};

test("the handshake accept key matches the RFC 6455 worked example", () => {
  // From RFC 6455 §1.3 — if this drifts, no browser will complete a handshake.
  assert.equal(acceptKey("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
});

test("the GUID is the one the spec mandates", () => {
  assert.equal(WS_GUID, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
});

test("a short server frame is unmasked, FIN-set text", () => {
  const frame = encodeFrame("hi");
  assert.equal(frame[0], 0x81); // FIN + text opcode
  assert.equal(frame[1], 2); // length, mask bit clear — servers never mask
  assert.equal(frame.subarray(2).toString(), "hi");
});

test("a medium payload uses the 16-bit length path", () => {
  const text = "x".repeat(200);
  const frame = encodeFrame(text);
  assert.equal(frame[1], 126);
  assert.equal(frame.readUInt16BE(2), 200);
  assert.equal(frame.subarray(4).toString(), text);
});

test("a large payload uses the 64-bit length path", () => {
  const text = "y".repeat(70_000);
  const frame = encodeFrame(text);
  assert.equal(frame[1], 127);
  assert.equal(Number(frame.readBigUInt64BE(2)), 70_000);
  assert.equal(frame.subarray(10).length, 70_000);
});

test("a realistic event payload survives a frame round trip", () => {
  // Guards the 126-byte boundary, which a real fh-event/1 sits just past.
  const event = JSON.stringify({
    schema: "fh-event/1",
    id: crypto.randomUUID(),
    actor: { pseudo: "Sol", character: "Yedrivel" },
    display: { title: "Hunting", total: 18, parts: [{ k: "d20", v: "18" }] },
  });
  const frame = encodeFrame(event);
  const carried = frame[1] === 126 ? frame.subarray(4) : frame.subarray(2);
  assert.equal(carried.toString("utf8"), event);
});

test("a masked client frame is unmasked correctly", () => {
  const { frames, rest } = decodeFrames(clientFrame("hello"));
  assert.equal(frames.length, 1);
  assert.equal(frames[0].opcode, OPCODE.text);
  assert.equal(frames[0].payload.toString(), "hello");
  assert.equal(rest.length, 0);
});

test("several frames arriving in one chunk are all decoded", () => {
  const buf = Buffer.concat([clientFrame("one"), clientFrame("two"), clientFrame("three")]);
  const { frames } = decodeFrames(buf);
  assert.deepEqual(frames.map((f) => f.payload.toString()), ["one", "two", "three"]);
});

test("a frame split across TCP chunks is held until complete, not corrupted", () => {
  const whole = clientFrame("split-me-please");
  const head = whole.subarray(0, 6);
  const tail = whole.subarray(6);
  const first = decodeFrames(head);
  assert.equal(first.frames.length, 0); // incomplete is normal, not an error
  const second = decodeFrames(Buffer.concat([first.rest, tail]));
  assert.equal(second.frames[0].payload.toString(), "split-me-please");
});

test("a close frame is surfaced by opcode", () => {
  const { frames } = decodeFrames(clientFrame("", OPCODE.close));
  assert.equal(frames[0].opcode, OPCODE.close);
});

test("an absurdly large declared length is refused rather than allocated", () => {
  const head = Buffer.alloc(10);
  head[0] = 0x81;
  head[1] = 127;
  head.writeBigUInt64BE(9_000_000_000n, 2);
  const { overflow } = decodeFrames(head);
  assert.equal(overflow, true);
});

test("the handshake rejects a wrong protocol version", () => {
  const res = handshakeResponse({ headers: { "sec-websocket-key": "abc", "sec-websocket-version": "8" } }, () => true);
  assert.equal(res.ok, false);
});

test("the handshake rejects a missing key", () => {
  const res = handshakeResponse({ headers: { "sec-websocket-version": "13" } }, () => true);
  assert.equal(res.ok, false);
});

test("a browser Origin outside the allow-list is refused", () => {
  // A WebSocket upgrade gets no CORS treatment from the browser, so this check
  // is the only origin gate that exists. Regressing it silently opens the feed.
  const res = handshakeResponse(
    { headers: { "sec-websocket-key": "abc", "sec-websocket-version": "13", origin: "https://evil.example" } },
    (o) => o === "https://noirchicot.github.io",
  );
  assert.equal(res.ok, false);
  assert.match(res.status, /403/);
});

test("the dock's own origin completes the handshake", () => {
  const res = handshakeResponse(
    { headers: { "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==", "sec-websocket-version": "13", origin: "https://noirchicot.github.io" } },
    (o) => o === "https://noirchicot.github.io",
  );
  assert.equal(res.ok, true);
  assert.match(res.headers, /101 Switching Protocols/);
  assert.match(res.headers, /s3pPLMBiTxaQ9kYGzzhZRbK\+xOo=/);
});

test("a Chrome extension service worker completes the real origin-gated handshake", () => {
  const res = handshakeResponse(
    {
      headers: {
        "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==",
        "sec-websocket-version": "13",
        origin: `chrome-extension://${"a".repeat(32)}`,
      },
    },
    isAllowedOrigin,
  );
  assert.equal(res.ok, true);
  assert.match(res.headers, /101 Switching Protocols/);
});

test("a non-browser client sending no Origin is allowed through", () => {
  // curl and the Node test harness send no Origin. The browser extension has
  // its chrome-extension:// origin checked explicitly by the test above.
  const res = handshakeResponse(
    { headers: { "sec-websocket-key": "abc", "sec-websocket-version": "13" } },
    () => false,
  );
  assert.equal(res.ok, true);
});
