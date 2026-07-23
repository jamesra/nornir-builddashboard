"use strict";

const LOG_DOM_MAX = 500;

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

function progressFractionToPercent(fraction) {
  if (fraction === null || fraction === undefined) return 0;
  return Math.max(0, Math.min(1, fraction)) * 100;
}

function progressTracksList(run) {
  if (!run.progress_tracks || typeof run.progress_tracks !== "object") return [];
  return Object.values(run.progress_tracks);
}

function isSingleItemTrack(track) {
  return track && track.total === 1;
}

function formatTrackStatusLabel(track, fraction) {
  if (isSingleItemTrack(track)) {
    return track.label || "1 item";
  }
  const pct = progressFractionToPercent(fraction);
  if (track.total != null) {
    return `${track.current || 0}/${track.total} (${pct.toFixed(0)}%)`;
  }
  if (fraction !== null && fraction !== undefined) {
    return `${pct.toFixed(0)}%`;
  }
  return "-";
}

function sidebarProgressDisplay(run) {
  const tracks = progressTracksList(run);
  const singleTrack = tracks.length === 1 && isSingleItemTrack(tracks[0]) ? tracks[0] : null;
  if (singleTrack) {
    return { showBar: false, label: singleTrack.label || "1 item" };
  }
  const fraction = computeProgressFraction(run);
  const pct = progressFractionToPercent(fraction);
  let label = "";
  if (run.progress_total === 1) {
    label = run.current_element || run.current_section || "1 item";
    return { showBar: false, label };
  }
  if (run.progress_total) {
    label = `${run.progress_current || 0}/${run.progress_total}`;
  }
  return { showBar: true, pct, label };
}

function formatCompute(compute) {
  if (!compute) return "-";
  const lower = String(compute).toLowerCase();
  if (lower === "cupy") return "CuPy";
  if (lower === "numpy") return "NumPy";
  return compute;
}

// -- run list ---------------------------------------------------------------

function isActiveRun(run) {
  return !["completed", "failed", "skipped", "stale"].includes(run.status || "running");
}

function runStartTs(run) {
  return run.start_ts || run.first_seen || 0;
}

function runLastSeenTs(run) {
  return run.last_seen || run.first_seen || 0;
}

function compareRuns(a, b) {
  const aActive = isActiveRun(a) ? 0 : 1;
  const bActive = isActiveRun(b) ? 0 : 1;
  if (aActive !== bActive) return aActive - bActive;

  if (aActive === 0) {
    const activity = runLastSeenTs(b) - runLastSeenTs(a);
    if (activity !== 0) return activity;
  }

  return runStartTs(b) - runStartTs(a);
}

function upsertRun(run) {
  if (!run || !run.run_id) return;
  state.runs.set(run.run_id, run);
}

let _rlPending = false;

function requestRenderRunList() {
  if (_rlPending) return;
  _rlPending = true;
  requestAnimationFrame(() => {
    _rlPending = false;
    renderRunList();
  });
}

function renderRunList() {
  const ul = el("runs");
  const filter = state.runFilter.toLowerCase();
  const runs = Array.from(state.runs.values()).sort(compareRuns);

  ul.innerHTML = "";
  for (const run of runs) {
    const hay = `${run.pipeline || ""} ${run.volumepath || ""} ${run.run_id}`.toLowerCase();
    if (filter && !hay.includes(filter)) continue;

    const li = document.createElement("li");
    li.className = "run-item" + (run.run_id === state.selectedRunId ? " selected" : "");
    li.dataset.runId = run.run_id;
    li.onclick = () => selectRun(run.run_id);

    const status = run.status || "running";
    const started = run.start_ts || run.first_seen;
    const runtime = fmtDuration(runRuntimeSeconds(run));
    const sidebarProgress = sidebarProgressDisplay(run);
    const pct = sidebarProgress.pct || 0;
    const progressLabel = sidebarProgress.label || "";
    const progressBarHtml = sidebarProgress.showBar
      ? `<div class="r-progress"><div class="r-progress-fill" style="width:${pct.toFixed(1)}%"></div></div>`
      : "";

    li.innerHTML = `
      <div class="r-line1">
        <span class="r-pipeline">${escapeHtml(run.pipeline || "(pipeline)")}</span>
        <div class="r-line1-actions">
          <span class="badge ${statusClass(status)}">${escapeHtml(status)}</span>
          <button type="button" class="run-delete" title="Delete run"
                  aria-label="Delete run">
            <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">
              <path fill="currentColor"
                    d="M6 2h4l.5 1H14v1H2V3h3.5L6 2zm1 4v6H6V6h1zm3 0v6H9V6h1zM3.5 5h9l-.7 9.1A1 1 0 0 1 10.8 15H5.2a1 1 0 0 1-1-.9L3.5 5z"/>
            </svg>
          </button>
        </div>
      </div>
      <div class="r-volume" title="${escapeHtml(run.volumepath || "")}">${escapeHtml(run.volumepath || "")}</div>
      <div class="r-meta">STARTED ${escapeHtml(fmtDateTime(started) || "-")} · ${escapeHtml(runtime)}</div>
      <div class="r-counts">
        <span class="err">${run.error_count || 0} err</span>
        &middot; <span class="warn">${run.warning_count || 0} warn</span>
      </div>
      ${progressBarHtml}
      <div class="r-progress-label">${escapeHtml(progressLabel)}</div>`;

    const deleteBtn = li.querySelector(".run-delete");
    deleteBtn.addEventListener("click", (event) => {
      event.stopPropagation();
      void deleteRun(run);
    });

    ul.appendChild(li);
  }
}

function tickRuntimes() {
  if (state.selectedRunId) {
    const run = state.runs.get(state.selectedRunId);
    if (run) {
      el("d-runtime").textContent = fmtDuration(runRuntimeSeconds(run));
    }
  }
  for (const li of el("runs").querySelectorAll(".run-item[data-run-id]")) {
    const run = state.runs.get(li.dataset.runId);
    if (run && isActiveRun(run)) {
      const meta = li.querySelector(".r-meta");
      if (meta) {
        const started = run.start_ts || run.first_seen;
        meta.textContent = `STARTED ${fmtDateTime(started) || "-"} · ${fmtDuration(runRuntimeSeconds(run))}`;
      }
    }
  }
}

function removeRunFromUi(runId) {
  state.runs.delete(runId);
  if (state.selectedRunId === runId) {
    state.selectedRunId = null;
    el("detail").classList.add("hidden");
    el("detail-empty").classList.remove("hidden");
    el("log").innerHTML = "";
  }
  requestRenderRunList();
}

async function deleteRun(run) {
  if (!run || !run.run_id) return;

  if (isActiveRun(run)) {
    const label = run.pipeline || run.run_id;
    const confirmed = window.confirm(
      `Delete running build "${label}"?\n\nThis removes it from the dashboard only; the build process itself is not stopped.`);
    if (!confirmed) return;
  }

  const runId = run.run_id;
  try {
    const resp = await fetch(`/api/runs/${encodeURIComponent(runId)}`, { method: "DELETE" });
    if (resp.status === 404) {
      removeRunFromUi(runId);
      return;
    }
    if (!resp.ok) {
      const body = await resp.text();
      window.alert(`Failed to delete run: ${resp.status} ${body}`);
      return;
    }
    // WebSocket run_deleted also updates clients; apply locally for immediate feedback.
    removeRunFromUi(runId);
  } catch (err) {
    window.alert(`Failed to delete run: ${err}`);
  }
}

// -- detail -----------------------------------------------------------------

function isTerminalStatus(status) {
  return ["completed", "failed", "skipped", "stale"].includes(status || "");
}

function renderProgressTracks(run) {
  const container = el("d-progress-tracks");
  container.innerHTML = "";

  if (isTerminalStatus(run.status)) {
    return;
  }

  const tracks = progressTracksList(run);

  if (!tracks.length) {
    // Fallback single bar from top-level progress fields.
    const fraction = computeProgressFraction(run);
    const pct = progressFractionToPercent(fraction);
    const fallbackTrack = {
      label: "Progress",
      total: run.progress_total,
      current: run.progress_current,
      fraction,
    };
    if (run.progress_total === 1) {
      fallbackTrack.label = run.current_element || run.current_section || "1 item";
    }
    const label = formatTrackStatusLabel(fallbackTrack, fraction);
    const track = document.createElement("div");
    track.className = "progress-track";
    if (isSingleItemTrack(fallbackTrack)) {
      track.innerHTML = `<div class="progress-track-label progress-track-single">${escapeHtml(label)}</div>`;
    } else {
      track.innerHTML =
        `<div class="progress-track-label">Progress</div>` +
        `<div class="progress-wrap">` +
        `<div class="progress-bar"><div class="progress-fill" style="width:${pct.toFixed(1)}%"></div></div>` +
        `<span class="progress-label">${escapeHtml(label)}</span></div>`;
    }
    container.appendChild(track);
    return;
  }

  const sorted = tracks.slice().sort((a, b) => (a.depth || 0) - (b.depth || 0));
  for (const track of sorted) {
    let fraction = track.fraction;
    if ((fraction === null || fraction === undefined) && track.total) {
      fraction = (track.current || 0) / track.total;
    }
    const pct = progressFractionToPercent(fraction);
    const totalHint = track.total != null ? ` (${track.total} total)` : "";
    const title = `${track.label || "progress"}${totalHint}`;
    const label = formatTrackStatusLabel(track, fraction);

    const row = document.createElement("div");
    row.className = "progress-track";
    if (isSingleItemTrack(track)) {
      row.innerHTML = `<div class="progress-track-label progress-track-single">${escapeHtml(label)}</div>`;
    } else {
      row.innerHTML =
        `<div class="progress-track-label">${escapeHtml(title)}</div>` +
        `<div class="progress-wrap">` +
        `<div class="progress-bar"><div class="progress-fill" style="width:${pct.toFixed(1)}%"></div></div>` +
        `<span class="progress-label">${escapeHtml(label)}</span></div>`;
    }
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
  if (isTerminalStatus(status)) {
    el("d-stage").textContent = "-";
    el("d-section").textContent = "-";
  } else {
    el("d-stage").textContent = run.current_stage || "-";
    el("d-section").textContent = run.current_section || run.current_element || "-";
  }
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

function updateJumpButtonVisibility() {
  const newest = el("jump-newest");
  const oldest = el("jump-oldest");
  if (!newest || !oldest) return;
  if (state.logTailPinned) {
    newest.classList.add("hidden");
  } else {
    newest.classList.remove("hidden");
  }
  const log = el("log");
  if (log && log.children.length > 0) {
    oldest.classList.remove("hidden");
  } else {
    oldest.classList.add("hidden");
  }
}

function isLogNearTop(log) {
  return log.scrollTop < 40;
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
  while (log.children.length > LOG_DOM_MAX) {
    log.removeChild(log.lastChild);
  }
  if (state.logTailPinned) {
    log.scrollTop = 0;
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

function renderEvents(events) {
  for (const event of events) {
    state.lastEventId = Math.max(state.lastEventId, event.id);
    appendLogLine(event);
  }
}

async function selectRun(runId) {
  state.selectedRunId = runId;
  state.lastEventId = 0;
  state.logTailPinned = true;
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
  renderEvents(evData.events || []);

  const log = el("log");
  log.scrollTop = 0;
  updateJumpButtonVisibility();
}

// -- websocket --------------------------------------------------------------

async function fetchMissedEventsForSelectedRun() {
  const runId = state.selectedRunId;
  if (!runId) return;

  const resp = await fetch(
    `/api/runs/${encodeURIComponent(runId)}/events?after_id=${state.lastEventId}&limit=5000`
  );
  const data = await resp.json();
  renderEvents((data.events || []).filter(e => e.id > state.lastEventId));

  const runResp = await fetch(`/api/runs/${encodeURIComponent(runId)}`);
  const runData = await runResp.json();
  if (runData.run) {
    upsertRun(runData.run);
    renderHeader(runData.run);
  }
  if (state.logTailPinned) {
    const log = el("log");
    log.scrollTop = 0;
  }
}

async function refreshRunList() {
  const resp = await fetch("/api/runs");
  const data = await resp.json();
  for (const run of data.runs) upsertRun(run);
  requestRenderRunList();
}

function connectWs() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    const c = el("connection");
    c.textContent = "live";
    c.className = "conn conn-up";
    // After dashboard rebuild/restart the socket reconnects before the user
    // refreshes; re-load run summaries so retained MQTT + SQLite state appear.
    refreshRunList()
      .then(() => fetchMissedEventsForSelectedRun())
      .catch(() => {});
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
      removeRunFromUi(data.run_id);
      return;
    }

    if (data.type !== "event") return;

    if (data.run) upsertRun(data.run);
    requestRenderRunList();

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
    requestRenderRunList();
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
    if (state.logTailPinned && !isLogNearTop(log)) {
      state.logTailPinned = false;
      updateJumpButtonVisibility();
    }
  });

  el("jump-newest").addEventListener("click", () => {
    state.logTailPinned = true;
    log.scrollTop = 0;
    updateJumpButtonVisibility();
  });

  el("jump-oldest").addEventListener("click", () => {
    log.scrollTop = log.scrollHeight;
  });

  await refreshRunList();

  connectWs();
  // Keep runtime labels fresh for live runs without rebuilding the run list.
  setInterval(tickRuntimes, 1000);
}

init();
