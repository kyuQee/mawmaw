
# MAWMAW Configuration Guide (Explicit Routing)

## Overview

MAWMAW now gives you **complete control** over your data flow.  
Instead of fan‑out broadcasting and automatic re‑ingestion, you define explicit routes:

- Every **stream** (ingestor‑produced or script‑emitted) **must** have a route in the `[routes]` section.
- A route tells the system exactly **which scripts** and **which publisher endpoints** should receive that stream.
- If a stream has **no route**, it is **dropped** – no processing, no output.

This makes your pipeline **predictable**, **testable**, and **efficient**.

---

## Section Reference

### 1. `[ingestor]` – Data Sources

| Key      | Required | Description |
|----------|----------|-------------|
| `id`     | yes      | Unique identifier for this ingestor instance. |
| `plugin` | yes      | Path to the shared library plugin (`.so`). |
| `as`     | no       | **Rename** the stream that this ingestor produces. Without `as`, the plugin’s hardcoded stream name is used. Use `as` to disambiguate multiple ingestors that would otherwise produce the same stream name. |

**Example:**
```ini
[ingestor]
id     = nyse_feed
plugin = ./plugin_nyse.so
as     = nyse_trades      # now the stream is called "nyse_trades"
```

---

### 2. `[publisher]` – Output Endpoints

Each publisher has a **unique `id`** and a **`type`**.  
You can create as many publishers as you like – they only receive events that are explicitly routed to them.

| Type        | Required keys | Description |
|-------------|---------------|-------------|
| `stdout`    | `id`          | Prints event metadata to standard output. |
| `null`      | `id`          | Discards all events (benchmarking). |
| `file`      | `id`, `path`  | Appends events to the specified file. |
| `telemetry` | `id`          | Renders the live telemetry dashboard (clears screen). |
| `websocket` | `id`, `port`  | Broadcasts events as JSON to all connected WebSocket clients. Optional `host` (default `0.0.0.0`). |

**Examples:**
```ini
[publisher]
id   = stdout
type = stdout

[publisher]
id   = alerts_log
type = file
path = ./alerts.log

[publisher]
id   = ws_live
type = websocket
port = 9001
host = 0.0.0.0        # optional
```

---

### 3. `[script]` – Processing Units

Scripts are the heart of your pipeline. They consume one stream (the `trigger`) and can produce one or more output streams (`emits`).  
The script itself does **not** route its own output – you define where its emitted streams go in the `[routes]` section.

| Key           | Required | Description |
|---------------|----------|-------------|
| `id`          | yes      | Unique identifier for this script. Used in routes. |
| `runtime`     | yes      | `python` or `wasm`. |
| `path`        | yes      | Path to the script file (`.py` or `.wasm`). |
| `trigger`     | yes      | The stream this script **consumes**. |
| `emits`       | no       | Comma‑separated list of streams this script may emit. (Informative; routing is still explicit.) |
| `window`      | no       | Defines a window on the trigger stream (count or time based). Can appear multiple times. Format: `stream_id, count|time, N`. |

**Example:**
```ini
[script]
id      = signal_detector
runtime = python
path    = ./scripts/signal.py
trigger = raw_trades
emits   = signals, anomalies
window  = raw_trades, count, 64
```

> **Note on `emits`**: This field is **optional**. It tells the system which stream names this script *may* produce, but it does **not** route them. You still need a `[routes]` entry for each emitted stream.

---

### 4. `[routes]` – The Explicit Routing Table

This section is where you define **exactly** what happens to every stream in your system.

- **Format:** `stream_id = target1, target2, ...`
- **Targets** can be:
  - **Script `id`** – the event will be enqueued to that script’s worker thread.
  - **Publisher `id`** – the event will be forwarded to that output endpoint.

A single stream can have multiple targets (both scripts and publishers).  
If a stream is not listed in the `[routes]` section, it is **silently dropped**.

**Example:**
```ini
[routes]
# Ingestor stream → script + publisher
nyse_trades = signal_detector, stdout

# Script output → another script + websocket
signals     = alert_formatter, ws_live

# Another stream → only a file
anomalies   = alerts_log
```

---

## Putting It All Together – A Complete Example

```ini
# ── Ingestors ──────────────────────────────────────────────────
[ingestor]
id     = market
plugin = ./plugin_market.so
as     = raw_trades

# ── Publishers ──────────────────────────────────────────────────
[publisher]
id   = stdout
type = stdout

[publisher]
id   = live_dash
type = websocket
port = 9001

[publisher]
id   = telemetry
type = telemetry

[publisher]
id   = archive
type = file
path = ./trades.log

# ── Scripts ──────────────────────────────────────────────────
[script]
id      = signal_detector
runtime = python
path    = ./scripts/detect.py
trigger = raw_trades
emits   = signals
window  = raw_trades, count, 100

[script]
id      = alert_formatter
runtime = python
path    = ./scripts/format.py
trigger = signals
emits   = formatted_alerts
window  = signals, time, 5000000000

# ── Routes ──────────────────────────────────────────────────
[routes]
# raw_trades goes to the detector AND both stdout and archive
raw_trades         = signal_detector, stdout, archive

# signals (from detector) goes to the formatter AND live dashboard
signals            = alert_formatter, live_dash

# formatted_alerts (from formatter) goes to stdout and telemetry
formatted_alerts   = stdout, telemetry
```

---

## Important Details

- **Renaming ingestors** with `as` is **strongly recommended** if you run more than one ingestor, to avoid stream name collisions.
- **The telemetry publisher** is a special endpoint that renders the dashboard. It will only display metrics for streams that are **explicitly routed** to its `id` (`telemetry` in the example).
- **Re‑ingestion is now explicit**: A script’s emitted stream is just another stream. To have it processed further, you **must** route it to another script in the `[routes]` section.
- **No automatic fan‑out**: If you want an event to go to multiple places, list all targets in the route.
- **Validation**: The system will warn you about routes that point to unknown script or publisher IDs.

---

## Migration from Old (Broadcast) Config

If you have an existing `mawmaw.conf` without a `[routes]` section:

1. Add a `[routes]` section.
2. For every stream you care about, add an entry that lists all scripts that previously matched that stream via `trigger`, plus all publishers you want to receive it.
3. Remove any assumptions about automatic re‑ingestion – you now control the loop explicitly.

Example:
```ini
# Old: every script with trigger "raw_trades" would receive it,
# and every publisher would get everything.

# New: explicit route
[routes]
raw_trades = detector, logger, stdout, file_log
```

---

## Troubleshooting

| Problem | Likely Cause | Solution |
|---------|--------------|----------|
| Events are not reaching a script | The stream is not routed to that script’s `id`. | Add the script `id` to the route for that stream. |
| Events are not reaching a publisher | The publisher `id` is not listed in the route. | Add the publisher `id` to the route. |
| Telemetry dashboard is empty | No stream is routed to the `telemetry` publisher. | Add `telemetry` as a target for a stream that you want to appear in the dashboard. |
| Unexpected loops or excessive lineage depth | A script emits a stream that routes back to itself (or to another script that eventually routes back). | Break the loop by using different stream names or not routing the emitted stream back to the same script. |

---

## Summary

- **You decide** which stream goes to which script and which publisher.
- **No more guesswork** – every event follows a clear, configurable path.
- **Fail‑safe**: Missing routes = dropped events (so you know exactly what’s happening).
