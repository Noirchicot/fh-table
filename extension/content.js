const ID = "fh-abovevtt-bridge-status";

const render = (state) => {
  let pill = document.getElementById(ID);
  if (!pill) {
    pill = document.createElement("div");
    pill.id = ID;
    pill.setAttribute("role", "status");
    pill.setAttribute("aria-live", "polite");
    document.documentElement.appendChild(pill);
  }
  const status = state?.status || "off";
  pill.dataset.status = status;
  pill.textContent = !state?.enabled
    ? "FH bridge · OFF"
    : status === "live"
      ? `FH ${state.code} · LIVE · ${state.delivered || 0} line(s)`
      : `FH ${state.code || "—"} · NOT DELIVERING · ${state?.error || "connecting…"}`;
};

chrome.runtime.onMessage.addListener((message) => {
  if (message?.type === "fh-bridge-status") render(message.state);
});

chrome.runtime.sendMessage({ type: "fh-bridge-content-ready" }).then(render).catch(() => {
  render({ enabled: true, status: "error", error: "extension service worker unavailable" });
});
