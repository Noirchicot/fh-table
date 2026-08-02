# Fate's Hand — AboveVTT Bridge (package 12b)

Chrome/Edge extension for the DM's machine. It subscribes to the table server on
loopback and posts **plain resolved text** into AboveVTT's game log. It never
sends slash commands or dice expressions, so AboveVTT never rolls a second,
divergent result.

## Load for the live test

1. Start `node table-server.mjs FH2` from the `fh-table` repo.
2. Open `chrome://extensions`, enable Developer mode, choose **Load unpacked**,
   and select this `extension/` directory.
3. Open the campaign in AboveVTT as DM. A small `FH bridge · OFF` pill should be
   visible at bottom-left.
4. Open the extension popup, enter `FH2`, and choose **Connect**.
5. The popup, action badge, and page pill must all say `LIVE` before rolling.

If the table server, AboveVTT object, DM role, or AboveVTT game socket is missing,
the bridge shows `NOT DELIVERING` and refuses to advance its feed cursor. The
event is replayed after reconnection; it is never silently discarded.

## Local test without AboveVTT

Run a detached local server and the shared-core subscriber in separate shells:

```text
node table-server.mjs TEST --no-mirror --no-tunnel --port 8792
node extension/dev-harness.mjs TEST 8792
```

POST fake `fh-event/1` inputs to `/feed/TEST`. The harness prints exactly what
the extension will hand to AboveVTT. Repeated event ids print once; higher
revisions print an explicit `UPDATE ... supersedes ...` line.

## Compatibility contract

- Manifest V3, Chrome/Edge 116+.
- AboveVTT 1.59 source pin: `8bcadd82fd88eef31b40ba60b630aae5de5ceb7e`.
- Required AboveVTT internals: `window.MB.inject_chat`, an open `window.MB.ws`,
  and `window.DM === true`.
- Event contract: `fh-event/1`; machines read `intent`, while `display` remains
  a presentation fallback only.
