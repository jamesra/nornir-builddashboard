"use strict";

const state = {
  runs: new Map(),       // run_id -> summary
  selectedRunId: null,
  lastEventId: 0,        // highest event id rendered for the selected run
  runFilter: "",
  logSearch: "",
  levels: new Set(["error", "warning", "info", "event", "status"]),
  logTailPinned: true,
};

const el = (id) => document.getElementById(id);

function fmtTime(ts) {
  if (!ts) return "";
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString();
}

function fmtDateTime(ts) {
  if (!ts) return "";
  const d = new Date(ts * 1000);
  return d.toLocaleString(undefined, {
    month: "short", day: "numeric",
    hour: "numeric", minute: "2-digit", second: "2-digit",
  });
}

function fmtDuration(seconds) {
  if (seconds == null || Number.isNaN(seconds) || seconds < 0) return "-";
  const s = Math.floor(seconds);
  const hrs = Math.floor(s / 3600);
  const mins = Math.floor((s % 3600) / 60);
  const secs = s % 60;
  if (hrs > 0) return `${hrs}h ${mins}m ${secs}s`;
  if (mins > 0) return `${mins}m ${secs}s`;
  return `${secs}s`;
}

function runRuntimeSeconds(run) {
  if (!run) return null;
  const start = run.start_ts || run.first_seen;
  if (!start) return null;
  const end = run.end_ts || (["completed", "failed", "skipped", "stale"].includes(run.status)
    ? run.last_seen
    : (Date.now() / 1000));
  return end - start;
}

function statusClass(status) {
  switch (status) {
    case "completed": return "completed";
    case "failed": return "failed";
    case "skipped": return "skipped";
    case "stale": return "stale";
    default: return "running";
  }
}

function computeProgressFraction(run) {
  let fraction = run.progress_fraction;
  if ((fraction === null || fraction === undefined) && run.progress_total) {
    fraction = (run.progress_current || 0) / run.progress_total;
  }
  return fraction;
}

function formatCompute(compute) {
  if (!compute) return "-";
  const lower = String(compute).toLowerCase();
  if (lower === "cupy") return "CuPy";
  if (lower === "numpy") return "NumPy";
  return compute;
}

// -- run list ---------------------------------------------------------------

function upsertRun(run) {
  if (!run || !run.run_id) return;
  state.runs.set(run.run_id, run);
}

function renderRunList() {
  const ul = el("runs");
  const filter = state.runFilter.toLowerCase();
  const runs = Array.from(state.runs.values()).sort(
    (a, b) => (b.last_seen || 0) - (a.last_seen || 0)
  );

  ul.innerHTML = "";
  for (const run of runs) {
    const hay = `${run.pipeline || ""} ${run.volumepath || ""} ${run.run_id}`.toLowerCase();
    if (filter && !hay.includes(filter)) continue;

    const li = document.createElement("li");
    li.className = "run-item" + (run.run_id === state.selectedRunId ? " selected" : "");
    li.onclick = () => selectRun(run.run_id);

    const status = run.status || "running";
    const started = run.start_ts || run.first_seen;
    const runtime = fmtDuration(runRuntimeSeconds(run));
    const fraction = computeProgressFraction(run);
    const pct = fraction ? Math.max(0, Math.min(1, fraction)) * 100 : 0;
    let progressLabel = "";
    if (run.progress_total) {
      progressLabel = `${run.progress_current || 0}/${run.progress_total}`;
    }

    li.innerHTML = `
      <div class="r-line1">
        <span class="r-pipeline">${escapeHtml(run.pipeline || "(pipeline)")}</span>
        <span class="badge ${statusClass(status)}">${escapeHtml(status)}</span>
      </div>
      <div class="r-volume" title="${escapeHtml(run.volumepath || "")}">${escapeHtml(run.volumepath || "")}</div>
      <div class="r-meta">STARTED ${escapeHtml(fmtDateTime(started) || "-")} · ${escapeHtml(runtime)}</div>
      <div class="r-counts">
        <span class="err">${run.error_count || 0} err</span>
        &middot; <span class="warn">${run.warning_count || 0} warn</span>
      </div>
      <div class="r-progress"><div class="r-progress-fill" style="width:${pct.toFixed(1)}%"></div></div>
      <div class="r-progress-label">${escapeHtml(progressLabel)}</div>`;
    ul.appendChild(li);
  }
}

// -- detail -----------------------------------------------------------------

function renderProgressTracks(run) {
  const container = el("d-progress-tracks");
  container.innerHTML = "";

  const tracks = run.progress_tracks && typeof run.progress_tracks === "object"
    ? Object.values(run.progress_tracks)
    : [];

  if (!tracks.length) {
    // Fallback single bar from top-level progress fields.
    const fraction = computeProgressFraction(run);
    const pct = fraction ? Math.max(0, Math.min(1, fraction)) * 100 : 0;
    let label = "-";
    if (run.progress_total) {
      label = `${run.progress_current || 0}/${run.progress_total} (${pct.toFixed(0)}%)`;
    } else if (fraction) {
      label = `${pct.toFixed(0)}%`;
    }
    const track = document.createElement("div");
    track.className = "progress-track";
    track.innerHTML =
      `<div class="progress-track-label">Progress</div>` +
      `<div class="progress-wrap">` +
      `<div class="progress-bar"><div class="progress-fill" style="width:${pct.toFixed(1)}%"></div></div>` +
      `<span class="progress-label">${escapeHtml(label)}</span></div>`;
    container.appendChild(track);
    return;
  }

  const sorted = tracks.slice().sort((a, b) => (a.depth || 0) - (b.depth || 0));
  for (const track of sorted) {
    let fraction = track.fraction;
    if ((fraction === null || fraction === undefined) && track.total) {
      fraction = (track.current || 0) / track.total;
    }
    const pct = fraction ? Math.max(0, Math.min(1, fraction)) * 100 : 0;
    const totalHint = track.total != null ? ` (${track.total} total)` : "";
    const title = `${track.label || "progress"}${totalHint}`;
    let label = "-";
    if (track.total != null) {
      label = `${track.current || 0}/${track.total} (${pct.toFixed(0)}%)`;
    } else if (fraction) {
      label = `${pct.toFixed(0)}%`;
    }

    const row = document.createElement("div");
    row.className = "progress-track";
    row.innerHTML =
      `<div class="progress-track-label">${escapeHtml(title)}</div>` +
      `<div class="progress-wrap">` +
      `<div class="progress-bar"><div class="progress-fill" style="width:${pct.toFixed(1)}%"></div></div>` +
      `<span class="progress-label">${escapeHtml(label)}</span></div>`;
    container.appendChild(row);
  }
}

function renderHeader(run) {
  if (!run) return;
  el("d-pipeline").textContent = run.pipeline || "(pipeline)";
  const status = run.status || "running";
  const badge = el("d-status");
  badge.textContent = status;
  badge.className = "badge " + statusClass(status);
  el("d-runid").textContent = run.run_id || "-";
  el("d-volume").textContent = run.volumepath || "-";
  el("d-host").textContent = run.host || "-";
  el("d-compute").textContent = formatCompute(run.compute);
  el("d-runtime").textContent = fmtDuration(runRuntimeSeconds(run));
  el("d-errors").textContent = run.error_count || 0;
  el("d-warnings").textContent = run.warning_count || 0;
  el("d-stage").textContent = run.current_stage || "-";
  el("d-section").textContent = run.current_section || run.current_element || "-";
  renderProgressTracks(run);
}

function logFilterKey(event) {
  if (event.kind === "log") return event.level || "info";
  if (event.kind === "event") return "event";
  if (event.kind === "status") return "status";
  return null; // meta / progress / other are not shown in the log pane
}

function formatEvent(event) {
  const p = event.payload || {};
  if (event.kind === "event") {
    const type = p.event || "event";
    const bits = [];
    if (p.function) bits.push(p.function);
    if (p.element) bits.push(p.element);
    if (p.section !== undefined && p.section !== null) bits.push("section " + p.section);
    if (p.current !== undefined && p.total !== undefined) bits.push(`${p.current}/${p.total}`);
    if (p.elapsed_s !== undefined) bits.push(`${Number(p.elapsed_s).toFixed(2)}s`);
    if (p.error) bits.push(p.error);
    return `${type} ${bits.join("  ")}`.trim();
  }
  if (event.kind === "status") {
    return `${p.topic ? p.topic + ": " : ""}${p.message || ""}`;
  }
  return (p.message || "").toString();
}

function updateJumpBottomVisibility() {
  const btn = el("jump-bottom");
  if (!btn) return;
  if (state.logTailPinned) btn.classList.add("hidden");
  else btn.classList.remove("hidden");
}

function isLogNearBottom(log) {
  return log.scrollHeight - log.scrollTop - log.clientHeight < 40;
}

function appendLogLine(event) {
  const key = logFilterKey(event);
  if (key === null) return;

  const line = document.createElement("div");
  line.className = "log-line " + (event.level || event.kind || "");
  line.dataset.key = key;
  line.dataset.text = formatEvent(event).toLowerCase();

  const tag = event.level || (event.kind === "event" ? "event" : event.kind);
  line.innerHTML =
    `<span class="t">${fmtTime(event.ts)}</span>` +
    `<span class="k">${escapeHtml(tag || "")}</span>` +
    `<span class="m">${escapeHtml(formatEvent(event))}</span>`;

  applyLineVisibility(line);

  const log = el("log");
  log.appendChild(line);
  if (state.logTailPinned) {
    log.scrollTop = log.scrollHeight;
  }
}

function applyLineVisibility(line) {
  const key = line.dataset.key;
  const matchLevel = state.levels.has(key);
  const matchSearch = !state.logSearch || line.dataset.text.includes(state.logSearch);
  line.style.display = matchLevel && matchSearch ? "" : "none";
}

function refilterLog() {
  document.querySelectorAll("#log .log-line").forEach(applyLineVisibility);
}

async function selectRun(runId) {
  state.selectedRunId = runId;
  state.lastEventId = 0;
  state.logTailPinned = true;
  updateJumpBottomVisibility();
  el("detail-empty").classList.add("hidden");
  el("detail").classList.remove("hidden");
  el("log").innerHTML = "";
  renderRunList();

  const runResp = await fetch(`/api/runs/${encodeURIComponent(runId)}`);
  const runData = await runResp.json();
  if (runData.run) {
    upsertRun(runData.run);
    renderHeader(runData.run);
  }

  const evResp = await fetch(`/api/runs/${encodeURIComponent(runId)}/events?limit=5000`);
  const evData = await evResp.json();
  for (const event of evData.events) {
    state.lastEventId = Math.max(state.lastEventId, event.id);
    appendLogLine(event);
  }
  const log = el("log");
  log.scrollTop = log.scrollHeight;
}

// -- websocket --------------------------------------------------------------

function connectWs() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    const c = el("connection");
    c.textContent = "live";
    c.className = "conn conn-up";
  };
  ws.onclose = () => {
    const c = el("connection");
    c.textContent = "disconnected";
    c.className = "conn conn-down";
    setTimeout(connectWs, 2000);
  };
  ws.onmessage = (msg) => {
    let data;
    try { data = JSON.parse(msg.data); } catch (_) { return; }

    if (data.type === "run_deleted") {
      state.runs.delete(data.run_id);
      if (state.selectedRunId === data.run_id) {
        state.selectedRunId = null;
        el("detail").classList.add("hidden");
        el("detail-empty").classList.remove("hidden");
        el("log").innerHTML = "";
      }
      renderRunList();
      return;
    }

    if (data.type !== "event") return;

    if (data.run) upsertRun(data.run);
    renderRunList();

    const event = data.event;
    if (event && event.run_id === state.selectedRunId) {
      if (data.run) renderHeader(data.run);
      if (event.id > state.lastEventId) {
        state.lastEventId = event.id;
        appendLogLine(event);
      }
    }
  };
}

// -- misc -------------------------------------------------------------------

function escapeHtml(value) {
  return String(value === null || value === undefined ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

async function init() {
  el("runfilter").addEventListener("input", (e) => {
    state.runFilter = e.target.value;
    renderRunList();
  });
  el("logsearch").addEventListener("input", (e) => {
    state.logSearch = e.target.value.toLowerCase();
    refilterLog();
  });
  document.querySelectorAll(".lvl").forEach((cb) => {
    cb.addEventListener("change", () => {
      if (cb.checked) state.levels.add(cb.value);
      else state.levels.delete(cb.value);
      refilterLog();
    });
  });

  const log = el("log");
  log.addEventListener("scroll", () => {
    if (isLogNearBottom(log)) {
      // Don't auto-re-pin on incidental near-bottom; only the button re-enables.
      return;
    }
    if (state.logTailPinned) {
      state.logTailPinned = false;
      updateJumpBottomVisibility();
    }
  });

  el("jump-bottom").addEventListener("click", () => {
    state.logTailPinned = true;
    log.scrollTop = log.scrollHeight;
    updateJumpBottomVisibility();
  });

  const resp = await fetch("/api/runs");
  const data = await resp.json();
  for (const run of data.runs) upsertRun(run);
  renderRunList();

  connectWs();
  // Keep runtime labels fresh for live runs.
  setInterval(() => {
    if (state.selectedRunId) {
      const run = state.runs.get(state.selectedRunId);
      if (run) {
        el("d-runtime").textContent = fmtDuration(runRuntimeSeconds(run));
      }
    }
    renderRunList();
  }, 1000);
}

init();
