const code = document.getElementById("code");
const status = document.getElementById("status");

const render = (state) => {
  if (state?.code && !code.value) code.value = state.code;
  status.dataset.status = state?.status || "off";
  status.textContent = !state?.enabled
    ? "Bridge stopped."
    : state.status === "live"
      ? `${state.code} · LIVE · ${state.delivered || 0} line(s) delivered`
      : `${state.code || "—"} · NOT DELIVERING · ${state.error || state.status || "connecting"}`;
};

document.getElementById("start").addEventListener("click", async () => {
  const result = await chrome.runtime.sendMessage({ type: "fh-bridge-start", code: code.value });
  if (!result?.ok) render({ enabled: true, status: "error", error: result?.error || "Could not start." });
  else render(result.state);
});

document.getElementById("stop").addEventListener("click", async () => {
  const result = await chrome.runtime.sendMessage({ type: "fh-bridge-stop" });
  render(result?.state || { enabled: false });
});

chrome.runtime.onMessage.addListener((message) => {
  if (message?.type === "fh-bridge-status") render(message.state);
});

chrome.runtime.sendMessage({ type: "fh-bridge-get-status" }).then(render);
