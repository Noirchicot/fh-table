#!/usr/bin/env node
// Local subscriber for package 12b. It exercises the same ledger/formatter as
// the extension against fh-table's real WebSocket, but prints instead of
// touching AboveVTT. No tunnel and no browser are required.

import { emptyLedger, planEvent } from "./bridge-core.mjs";

const code = String(process.argv[2] || "").trim().toUpperCase();
const port = Number(process.argv[3] || 8791);
if (!/^[A-Z0-9_-]{1,24}$/.test(code)) {
  console.error("usage: node extension/dev-harness.mjs <CAMPAIGN> [PORT]");
  process.exit(1);
}

let ledger = emptyLedger();
const socket = new WebSocket(`ws://127.0.0.1:${port}/feed/${encodeURIComponent(code)}/ws`);
socket.addEventListener("open", () => console.log(`[bridge harness] connected to ${code} on ${port}`));
socket.addEventListener("message", (message) => {
  const frame = JSON.parse(message.data);
  const plan = planEvent(ledger, frame.event);
  ledger = plan.nextLedger;
  if (plan.action === "deliver") console.log(plan.message);
});
socket.addEventListener("close", () => process.exit(0));
socket.addEventListener("error", () => {
  console.error("[bridge harness] table server unreachable");
  process.exitCode = 1;
});
