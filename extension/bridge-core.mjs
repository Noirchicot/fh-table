const MAX_SEEN_IDS = 800;
const MAX_ROLLS = 400;
export const KEEPALIVE_MS = 20_000;

// Chrome terminates an otherwise-idle Manifest V3 service worker after roughly
// 30 seconds. A WebSocket connection alone is not enough: an application-level
// message has to cross it inside that window. The table server deliberately
// ignores client text frames, so this heartbeat has no feed semantics.
export const startWebSocketKeepAlive = (
  getSocket,
  { setIntervalFn = setInterval, clearIntervalFn = clearInterval } = {},
) => {
  const timer = setIntervalFn(() => {
    const current = getSocket();
    if (current?.readyState === 1) current.send("keepalive");
  }, KEEPALIVE_MS);
  return () => clearIntervalFn(timer);
};

const text = (value, fallback = "") => {
  const out = typeof value === "string" ? value.trim() : value == null ? "" : String(value);
  return out.slice(0, 500) || fallback;
};

const number = (value) => {
  const out = Number(value);
  return Number.isFinite(out) ? out : null;
};

export const emptyLedger = () => ({ seenIds: [], rolls: {} });

export const normalizeLedger = (value) => {
  const seenIds = Array.isArray(value?.seenIds)
    ? value.seenIds.map((id) => text(id)).filter(Boolean).slice(-MAX_SEEN_IDS)
    : [];
  const rolls = {};
  if (value?.rolls && typeof value.rolls === "object" && !Array.isArray(value.rolls)) {
    for (const [key, row] of Object.entries(value.rolls).slice(-MAX_ROLLS)) {
      if (!key || !row || typeof row !== "object") continue;
      rolls[text(key)] = {
        rev: Math.max(0, Math.round(number(row.rev) ?? 0)),
        total: number(row.total),
        title: text(row.title, "Roll"),
        eventId: text(row.eventId),
      };
    }
  }
  return { seenIds, rolls };
};

export const escapeHtml = (value) =>
  text(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

const outcomeLabel = (value) =>
  ({
    success: "SUCCESS",
    failure: "FAILURE",
    "critical-success": "CRITICAL SUCCESS",
    "critical-failure": "CRITICAL FAILURE",
  })[value] || "";

const displaySummary = (event) => {
  const display = event.display && typeof event.display === "object" ? event.display : {};
  const actor = text(event.actor?.character || event.actor?.pseudo, "Unknown actor");
  const title = text(display.title, event.type === "note" ? "Note" : "Roll");
  const total = number(display.total);
  const parts = Array.isArray(display.parts)
    ? display.parts
        .slice(0, 12)
        .map((part) => `${text(part?.k)} ${text(part?.v)}`.trim())
        .filter(Boolean)
    : [];
  const badges = Array.isArray(display.badges) ? display.badges.slice(0, 8).map((badge) => text(badge)).filter(Boolean) : [];
  return {
    actor,
    title,
    total,
    line: [
      `${actor} — ${title}${total == null ? "" : `: ${total}`}`,
      parts.join(" · "),
      badges.join(" · "),
    ]
      .filter(Boolean)
      .join(" · "),
  };
};

const checkSummary = (event) => {
  const intent = event.intent;
  const actor = text(event.actor?.character || event.actor?.pseudo, "Unknown actor");
  const check = text(intent.check, text(event.display?.title, "Check"));
  const ability = text(intent.ability);
  const total = number(intent.total) ?? number(event.display?.total);
  const natural = number(intent.natural);
  const dc = number(intent.dc);
  const verdict = outcomeLabel(intent.outcome);
  return {
    actor,
    title: check,
    total,
    line: [
      `${actor} — ${check}${ability ? ` (${ability})` : ""}${total == null ? "" : `: ${total}`}`,
      natural == null ? "" : `d20 ${natural}`,
      dc == null ? "" : `DC ${dc}`,
      verdict,
    ]
      .filter(Boolean)
      .join(" · "),
  };
};

export const summarizeEvent = (event) => {
  if (event?.intent?.kind === "check") return checkSummary(event);
  const summary = displaySummary(event || {});
  if (event?.intent?.kind) summary.line += ` · [${text(event.intent.kind)} intent: printed only]`;
  return summary;
};

export const chatHtml = (message) => `<span><b>FH</b> · ${escapeHtml(message)}</span>`;

const appendSeen = (ledger, id) => {
  const ids = ledger.seenIds.filter((known) => known !== id);
  ids.push(id);
  return ids.slice(-MAX_SEEN_IDS);
};

const trimRolls = (rolls) => Object.fromEntries(Object.entries(rolls).slice(-MAX_ROLLS));

export const planEvent = (rawLedger, event) => {
  const ledger = normalizeLedger(rawLedger);
  const id = text(event?.id);
  if (!id || event?.schema !== "fh-event/1" || !event?.actor?.pseudo) {
    return { action: "reject", reason: "Malformed fh-event/1", nextLedger: ledger };
  }
  if (ledger.seenIds.includes(id)) return { action: "ignore", reason: "duplicate-id", nextLedger: ledger };

  const key = text(event.rollId || id);
  const rev = Math.max(0, Math.round(number(event.rev) ?? 0));
  const previous = ledger.rolls[key] || null;
  const seenIds = appendSeen(ledger, id);
  if (previous && rev <= previous.rev) {
    return { action: "ignore", reason: "stale-revision", nextLedger: { ...ledger, seenIds } };
  }

  const summary = summarizeEvent(event);
  const revised = !!previous;
  const message = revised
    ? `UPDATE r${rev} · ${summary.line}${previous.total == null ? "" : ` · supersedes ${previous.total}`}`
    : summary.line;
  const nextLedger = {
    seenIds,
    rolls: trimRolls({
      ...ledger.rolls,
      [key]: { rev, total: summary.total, title: summary.title, eventId: id },
    }),
  };
  return { action: "deliver", revised, message, html: chatHtml(message), nextLedger };
};

export const cleanCampaignCode = (value) => {
  const code = text(value).toUpperCase();
  return /^[A-Z0-9_-]{1,24}$/.test(code) ? code : "";
};

export const chooseAboveVttTab = (probes) => {
  const ready = (Array.isArray(probes) ? probes : []).filter((probe) => probe?.ready && probe?.connected && probe?.dm);
  if (!ready.length) return { ok: false, error: "No connected AboveVTT DM tab is available." };
  if (ready.length === 1) return { ok: true, tabId: ready[0].tabId };
  const active = ready.filter((probe) => probe.active);
  if (active.length === 1) return { ok: true, tabId: active[0].tabId };
  return { ok: false, error: "Several connected AboveVTT DM tabs are open; keep only the table tab active." };
};
