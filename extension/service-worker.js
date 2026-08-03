import {
  chooseAboveVttTab,
  cleanCampaignCode,
  emptyLedger,
  normalizeLedger,
  planEvent,
  startWebSocketKeepAlive,
} from "./bridge-core.mjs";

const STORAGE_KEY = "fhAboveVttBridge";
const DDB_URLS = ["https://www.dndbeyond.com/campaigns/*", "https://www.dndbeyond.com/characters*"];
const RETRY_MAX_MS = 30_000;

let state = {
  enabled: false,
  code: "",
  cursor: "",
  ledger: emptyLedger(),
  status: "off",
  error: "",
  delivered: 0,
};
let socket = null;
let retryTimer = null;
let retryMs = 1_000;
let deliveryChain = Promise.resolve();
let intentionalClose = false;
let stopKeepAlive = null;

const endKeepAlive = () => {
  if (stopKeepAlive) stopKeepAlive();
  stopKeepAlive = null;
};

const beginKeepAlive = () => {
  endKeepAlive();
  stopKeepAlive = startWebSocketKeepAlive(() => socket);
};

const publicState = () => ({
  enabled: state.enabled,
  code: state.code,
  cursor: state.cursor,
  status: state.status,
  error: state.error,
  delivered: state.delivered,
});

const persist = () => chrome.storage.local.set({ [STORAGE_KEY]: state });

const badge = async () => {
  const label = state.status === "live" ? "LIVE" : state.enabled ? "OFF" : "";
  await chrome.action.setBadgeText({ text: label });
  await chrome.action.setBadgeBackgroundColor({ color: state.status === "live" ? "#176b42" : "#a72d32" });
};

const broadcast = async () => {
  await badge();
  const tabs = await chrome.tabs.query({ url: DDB_URLS });
  await Promise.all(
    tabs.map((tab) => chrome.tabs.sendMessage(tab.id, { type: "fh-bridge-status", state: publicState() }).catch(() => {})),
  );
  chrome.runtime.sendMessage({ type: "fh-bridge-status", state: publicState() }).catch(() => {});
};

const setStatus = async (status, error = "") => {
  state.status = status;
  state.error = error;
  await persist();
  await broadcast();
};

const probeMainWorld = () => ({
  ready: !!(window.MB && typeof window.MB.inject_chat === "function"),
  connected: !!(window.MB?.ws && window.MB.ws.readyState === 1),
  dm: window.DM === true,
});

const injectMainWorld = async (payload) => {
  if (!window.MB || typeof window.MB.inject_chat !== "function") return { ok: false, error: "AboveVTT message bridge is unavailable." };
  if (window.DM !== true) return { ok: false, error: "The open AboveVTT tab is not the DM table." };
  if (!window.MB.ws || window.MB.ws.readyState !== 1) return { ok: false, error: "AboveVTT is visible but its game connection is offline." };
  try {
    // payload.inject carries whisper/sendTo/player/img always, and
    // rollType/rollTitle/result when the event's intent supports them
    // (extension/bridge-core.mjs injectionFields) — player/img fall back to
    // "Fate's Hand" / no portrait only when the roll's actor was never
    // resolved to a character (see fallback in injectionFields).
    await window.MB.inject_chat({
      ...payload.inject,
      player: payload.inject?.player || "Fate's Hand",
      img: payload.inject?.img || window.PLAYER_IMG || "",
      text: payload.html,
    });
    return { ok: true };
  } catch (error) {
    return { ok: false, error: `AboveVTT rejected the game-log line: ${error?.message || "unknown error"}` };
  }
};

const findAboveVttTab = async () => {
  const tabs = await chrome.tabs.query({ url: DDB_URLS });
  const probes = [];
  for (const tab of tabs) {
    try {
      const result = await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        world: "MAIN",
        func: probeMainWorld,
      });
      probes.push({ tabId: tab.id, active: !!tab.active, ...(result?.[0]?.result || {}) });
    } catch {
      probes.push({ tabId: tab.id, active: !!tab.active, ready: false, connected: false, dm: false });
    }
  }
  return chooseAboveVttTab(probes);
};

const deliverToAboveVtt = async (plan) => {
  const target = await findAboveVttTab();
  if (!target.ok) return target;
  try {
    const result = await chrome.scripting.executeScript({
      target: { tabId: target.tabId },
      world: "MAIN",
      func: injectMainWorld,
      args: [{ html: plan.html, inject: plan.inject }],
    });
    return result?.[0]?.result || { ok: false, error: "AboveVTT returned no delivery acknowledgement." };
  } catch (error) {
    return { ok: false, error: `Could not reach the AboveVTT tab: ${error?.message || "unknown error"}` };
  }
};

const closeForRetry = (error) => {
  endKeepAlive();
  if (socket) {
    const current = socket;
    socket = null;
    try {
      current.close();
    } catch {
      // The retry below is the recovery path.
    }
  }
  setStatus("error", error).finally(scheduleReconnect);
};

const handleFrame = async (raw) => {
  let frame;
  try {
    frame = JSON.parse(raw);
  } catch {
    return closeForRetry("The table server sent an unreadable WebSocket frame.");
  }
  const seq = typeof frame?.seq === "string" ? frame.seq : "";
  if (!seq || !frame?.event) return closeForRetry("The table server sent a frame without seq/event.");

  const plan = planEvent(state.ledger, frame.event);
  if (plan.action === "reject") {
    state.cursor = seq;
    await setStatus("error", `${plan.reason}; the event was refused, not injected.`);
    return;
  }
  if (plan.action === "ignore") {
    state.ledger = plan.nextLedger;
    state.cursor = seq;
    await persist();
    return;
  }

  const delivered = await deliverToAboveVtt(plan);
  if (!delivered.ok) return closeForRetry(delivered.error || "The game-log line was not delivered.");
  state.ledger = plan.nextLedger;
  state.cursor = seq;
  state.delivered += 1;
  await setStatus("live", "");
};

function scheduleReconnect() {
  if (!state.enabled || retryTimer) return;
  const delay = retryMs;
  retryMs = Math.min(RETRY_MAX_MS, retryMs * 2);
  retryTimer = setTimeout(() => {
    retryTimer = null;
    connect();
  }, delay);
}

function connect() {
  if (!state.enabled || !state.code || socket) return;
  intentionalClose = false;
  setStatus("connecting", "");
  const since = state.cursor ? `?since=${encodeURIComponent(state.cursor)}` : "";
  const next = new WebSocket(`ws://127.0.0.1:8791/feed/${encodeURIComponent(state.code)}/ws${since}`);
  socket = next;
  next.onopen = () => {
    if (socket !== next) return;
    retryMs = 1_000;
    beginKeepAlive();
    setStatus("live", "");
  };
  next.onmessage = (message) => {
    deliveryChain = deliveryChain.then(() => handleFrame(message.data)).catch((error) => {
      closeForRetry(`Bridge delivery failed: ${error?.message || "unknown error"}`);
    });
  };
  next.onerror = () => {
    if (socket === next) setStatus("error", "The local table server is unreachable.");
  };
  next.onclose = () => {
    if (socket !== next) return;
    endKeepAlive();
    socket = null;
    if (!intentionalClose && state.enabled) {
      setStatus("error", state.error || "The local table connection closed.").finally(scheduleReconnect);
    }
  };
}

const stop = async () => {
  intentionalClose = true;
  endKeepAlive();
  if (retryTimer) clearTimeout(retryTimer);
  retryTimer = null;
  if (socket) socket.close();
  socket = null;
  state.enabled = false;
  state.status = "off";
  state.error = "";
  await persist();
  await broadcast();
};

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (message?.type === "fh-bridge-get-status") {
    sendResponse(publicState());
    return;
  }
  if (message?.type === "fh-bridge-start") {
    const code = cleanCampaignCode(message.code);
    if (!code) {
      sendResponse({ ok: false, error: "Enter a valid campaign code." });
      return;
    }
    intentionalClose = true;
    endKeepAlive();
    if (socket) socket.close();
    socket = null;
    state = { ...state, enabled: true, code, cursor: "", ledger: emptyLedger(), status: "connecting", error: "", delivered: 0 };
    persist().then(() => {
      connect();
      sendResponse({ ok: true, state: publicState() });
    });
    return true;
  }
  if (message?.type === "fh-bridge-stop") {
    stop().then(() => sendResponse({ ok: true, state: publicState() }));
    return true;
  }
  if (message?.type === "fh-bridge-content-ready") {
    sendResponse(publicState());
  }
});

chrome.runtime.onInstalled.addListener(() => broadcast());
chrome.runtime.onStartup.addListener(() => connect());

chrome.storage.local.get(STORAGE_KEY).then((stored) => {
  const saved = stored?.[STORAGE_KEY];
  if (saved && typeof saved === "object") {
    state = {
      ...state,
      ...saved,
      code: cleanCampaignCode(saved.code),
      ledger: normalizeLedger(saved.ledger),
      status: saved.enabled ? "connecting" : "off",
      error: "",
    };
  }
  if (state.enabled && state.code) connect();
  else broadcast();
});
