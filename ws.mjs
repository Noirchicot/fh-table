// Minimal server-side WebSocket (RFC 6455), zero dependencies.
//
// WHY THIS EXISTS: the plan (§12.3) chose SSE for delivery and named WebSocket
// its pre-approved fallback. That fallback was called in on 2026-07-29, when a
// Cloudflare Quick Tunnel was measured delivering ZERO bytes of any streamed
// HTTP response body — isolated to the tunnel with a 10-line SSE server that
// worked perfectly on loopback. WebSocket through the same tunnel: connected in
// 290ms, first frame at 291ms, ticks on the millisecond. See plan §12.11.
//
// Only what the table server actually needs: a handshake, unfragmented text
// frames out, and enough frame parsing to honour a client's close and ping.
// No extensions, no compression, no fragmentation, no binary.

import crypto from "node:crypto";

export const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

export const OPCODE = { text: 0x1, close: 0x8, ping: 0x9, pong: 0xa };

export const acceptKey = (key) =>
  crypto.createHash("sha1").update(String(key) + WS_GUID).digest("base64");

// Server-to-client frames are never masked (RFC 6455 §5.1). Always FIN.
export const encodeFrame = (payload, opcode = OPCODE.text) => {
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), "utf8");
  let head;
  if (body.length < 126) {
    head = Buffer.from([0x80 | opcode, body.length]);
  } else if (body.length < 65_536) {
    head = Buffer.alloc(4);
    head[0] = 0x80 | opcode;
    head[1] = 126;
    head.writeUInt16BE(body.length, 2);
  } else {
    head = Buffer.alloc(10);
    head[0] = 0x80 | opcode;
    head[1] = 127;
    head.writeBigUInt64BE(BigInt(body.length), 2);
  }
  return Buffer.concat([head, body]);
};

// Consume as many complete frames as `buffer` holds; hand back the remainder so
// the caller can keep accumulating. A partial frame is normal on a TCP stream —
// it is not an error and must not be treated as one.
export const decodeFrames = (buffer) => {
  const frames = [];
  let offset = 0;
  for (;;) {
    if (buffer.length - offset < 2) break;
    const first = buffer[offset];
    const second = buffer[offset + 1];
    const opcode = first & 0x0f;
    const masked = (second & 0x80) !== 0;
    let length = second & 0x7f;
    let cursor = offset + 2;

    if (length === 126) {
      if (buffer.length - cursor < 2) break;
      length = buffer.readUInt16BE(cursor);
      cursor += 2;
    } else if (length === 127) {
      if (buffer.length - cursor < 8) break;
      const big = buffer.readBigUInt64BE(cursor);
      // A frame this large is not something this protocol ever sends us; refuse
      // rather than allocate against a hostile length.
      if (big > 1_000_000n) return { frames, rest: Buffer.alloc(0), overflow: true };
      length = Number(big);
      cursor += 8;
    }

    let mask = null;
    if (masked) {
      if (buffer.length - cursor < 4) break;
      mask = buffer.subarray(cursor, cursor + 4);
      cursor += 4;
    }

    if (buffer.length - cursor < length) break;
    const payload = Buffer.from(buffer.subarray(cursor, cursor + length));
    // Client-to-server frames MUST be masked (RFC 6455 §5.3); unmask in place.
    if (mask) for (let i = 0; i < payload.length; i += 1) payload[i] ^= mask[i % 4];
    frames.push({ opcode, payload });
    offset = cursor + length;
  }
  return { frames, rest: buffer.subarray(offset) };
};

// A browser does NOT apply CORS to a WebSocket upgrade — no preflight, no
// Access-Control-Allow-Origin, nothing. Any page anywhere can open a socket to
// a reachable server, so the Origin header is the only gate there is and this
// server must check it itself. Plan §12.9's allow-list, enforced here.
export const handshakeResponse = (req, isAllowedOrigin) => {
  const key = req.headers["sec-websocket-key"];
  const version = req.headers["sec-websocket-version"];
  if (!key || String(version) !== "13") return { ok: false, status: "HTTP/1.1 400 Bad Request" };
  const origin = req.headers.origin || "";
  // No Origin at all means a non-browser client (curl, the bridge, a test) —
  // those are reached only over loopback or the tunnel and carry the campaign
  // code, same trust model as the rest of the feed (plan §11.6).
  if (origin && !isAllowedOrigin(origin)) return { ok: false, status: "HTTP/1.1 403 Forbidden" };
  return {
    ok: true,
    headers:
      "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${acceptKey(key)}\r\n\r\n`,
  };
};
