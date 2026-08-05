"use strict";

/** Max log lines kept in the DOM (sliding window over retained history). */
const LOG_DOM_MAX = 5000;
/** Events fetched per history page (API clamps to 5000). */
const LOG_PAGE_SIZE = 2000;

const state = {
  runs: new Map(),       // run_id -> summary
  selectedRunId: null,
  lastEventId: 0,        // highest event id seen for the selected run
  oldestEventId: 0,      // lowest event id currently in the DOM window
  hasMoreOlder: false,
  loadingOlder: false,
  loadingLog: false,
  logSearchTimer: null,
  runFilter: "",
  logSearch: "",
  levels: new Set(["error", "warning", "info", "event", "status"]),
  logTailPinned: true,
  logNewestFirst: true,
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
    renderCurrentElementCard(null, true);
  } else {
    el("d-stage").textContent = run.current_stage || "-";
    renderCurrentElementCard(run, false);
  }
  renderProgressTracks(run);
}

function looksLikeAbsPath(value) {
  if (!value || typeof value !== "string") return false;
  return value.startsWith("/") || /^[A-Za-z]:[\\/]/.test(value) || value.startsWith("\\\\");
}

function pathBasename(path) {
  const normalized = String(path).replace(/\\/g, "/");
  const parts = normalized.split("/");
  return parts[parts.length - 1] || path;
}

async function copyPathToClipboard(path, button) {
  try {
    await navigator.clipboard.writeText(path);
    const previous = button.textContent;
    button.textContent = "Copied";
    window.setTimeout(() => {
      button.textContent = previous;
    }, 1200);
  } catch (_err) {
    window.prompt("Copy path:", path);
  }
}

function renderCurrentElementCard(run, terminal) {
  const host = el("d-section");
  host.replaceChildren();
  if (terminal || !run) {
    host.textContent = "-";
    return;
  }
  const path = run.current_path || (looksLikeAbsPath(run.current_element) ? run.current_element : null);
  if (path) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "path-copy-link";
    btn.textContent = pathBasename(path);
    btn.title = `${path} (click to copy)`;
    btn.addEventListener("click", () => {
      void copyPathToClipboard(path, btn);
    });
    host.appendChild(btn);
    return;
  }
  host.textContent = run.current_section || run.current_element || "-";
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
  const loadOlder = el("load-older");
  if (!newest || !oldest) return;
  if (state.logTailPinned && state.logNewestFirst) {
    newest.classList.add("hidden");
  } else {
    newest.classList.remove("hidden");
  }
  const log = el("log");
  if (log && log.children.length > 0 && state.logNewestFirst) {
    oldest.classList.remove("hidden");
  } else {
    oldest.classList.add("hidden");
  }
  if (loadOlder) {
    loadOlder.disabled = state.loadingOlder || !state.hasMoreOlder || !state.selectedRunId;
    loadOlder.textContent = state.hasMoreOlder
      ? (state.loadingOlder ? "Loading…" : "Load older")
      : "No older logs";
  }
}

function isLogNearTop(log) {
  return log.scrollTop < 40;
}

function isLogNearBottom(log) {
  return log.scrollHeight - log.scrollTop - log.clientHeight < 40;
}

function reverseLogOrder() {
  const log = el("log");
  const children = Array.from(log.children);
  if (children.length < 2) return;
  const frag = document.createDocumentFragment();
  for (let i = children.length - 1; i >= 0; i--) {
    frag.appendChild(children[i]);
  }
  log.appendChild(frag);
}

function checkedTypesParam() {
  return Array.from(state.levels).join(",");
}

function buildEventsQuery(extra) {
  const params = new URLSearchParams();
  params.set("limit", String(LOG_PAGE_SIZE));
  const types = checkedTypesParam();
  if (types) params.set("types", types);
  if (state.logSearch) params.set("q", state.logSearch);
  if (extra) {
    for (const [key, value] of Object.entries(extra)) {
      if (value != null && value !== "") params.set(key, String(value));
    }
  }
  return params.toString();
}

function eventMatchesActiveFilters(event) {
  const key = logFilterKey(event);
  if (key === null) return false;
  if (!state.levels.has(key)) return false;
  if (state.logSearch) {
    const text = formatEvent(event).toLowerCase();
    if (!text.includes(state.logSearch) &&
        !(JSON.stringify(event.payload || {}).toLowerCase().includes(state.logSearch))) {
      return false;
    }
  }
  return true;
}

function createLogLine(event) {
  const key = logFilterKey(event);
  if (key === null) return null;

  const line = document.createElement("div");
  line.className = "log-line " + (event.level || event.kind || "");
  line.dataset.key = key;
  line.dataset.id = String(event.id);
  line.dataset.text = formatEvent(event).toLowerCase();

  const tag = event.level || (event.kind === "event" ? "event" : event.kind);
  line.innerHTML =
    `<span class="t">${fmtTime(event.ts)}</span>` +
    `<span class="k">${escapeHtml(tag || "")}</span>` +
    `<span class="m">${escapeHtml(formatEvent(event))}</span>`;
  return line;
}

function trimLogWindow(preferKeepNewest) {
  const log = el("log");
  while (log.children.length > LOG_DOM_MAX) {
    if (preferKeepNewest) {
      // Drop oldest end of the window.
      if (state.logNewestFirst) {
        log.removeChild(log.lastChild);
      } else {
        log.removeChild(log.firstChild);
      }
    } else {
      // Drop newest end while browsing history.
      if (state.logNewestFirst) {
        log.removeChild(log.firstChild);
      } else {
        log.removeChild(log.lastChild);
      }
    }
  }
  syncWindowIdsFromDom();
}

function syncWindowIdsFromDom() {
  const log = el("log");
  let minId = 0;
  let maxId = state.lastEventId;
  for (const child of log.children) {
    const id = Number(child.dataset.id || 0);
    if (!id) continue;
    if (minId === 0 || id < minId) minId = id;
    if (id > maxId) maxId = id;
  }
  state.oldestEventId = minId;
  state.lastEventId = Math.max(state.lastEventId, maxId);
}

function appendLogLine(event, options) {
  const opts = options || {};
  const line = createLogLine(event);
  if (!line) return;

  const log = el("log");
  if (state.logNewestFirst) {
    if (opts.atOldestEnd) {
      log.appendChild(line);
    } else {
      log.insertBefore(line, log.firstChild);
    }
  } else if (opts.atOldestEnd) {
    log.insertBefore(line, log.firstChild);
  } else {
    log.appendChild(line);
  }

  trimLogWindow(state.logTailPinned || !opts.atOldestEnd);

  if (state.logTailPinned && !opts.atOldestEnd) {
    log.scrollTop = state.logNewestFirst ? 0 : log.scrollHeight;
  }
}

function renderEvents(events, options) {
  const opts = options || {};
  for (const event of events) {
    state.lastEventId = Math.max(state.lastEventId, event.id);
    if (state.oldestEventId === 0 || event.id < state.oldestEventId) {
      state.oldestEventId = event.id;
    }
    appendLogLine(event, opts);
  }
  updateJumpButtonVisibility();
}

async function fetchEventsPage(extra) {
  const runId = state.selectedRunId;
  if (!runId) return [];
  if (state.levels.size === 0) return [];
  const qs = buildEventsQuery(extra);
  const resp = await fetch(`/api/runs/${encodeURIComponent(runId)}/events?${qs}`);
  const data = await resp.json();
  return data.events || [];
}

async function reloadLogNewestPage() {
  const runId = state.selectedRunId;
  if (!runId) return;
  state.loadingLog = true;
  state.lastEventId = 0;
  state.oldestEventId = 0;
  state.hasMoreOlder = false;
  el("log").innerHTML = "";
  try {
    const events = await fetchEventsPage({});
    state.hasMoreOlder = events.length >= LOG_PAGE_SIZE;
    renderEvents(events, {});
    const log = el("log");
    if (state.logNewestFirst) {
      log.scrollTop = 0;
    } else {
      log.scrollTop = log.scrollHeight;
    }
    state.logTailPinned = true;
  } finally {
    state.loadingLog = false;
    updateJumpButtonVisibility();
  }
}

async function loadOlderEvents() {
  if (!state.selectedRunId || state.loadingOlder || !state.hasMoreOlder) return;
  if (state.levels.size === 0 || state.oldestEventId <= 0) return;

  state.loadingOlder = true;
  updateJumpButtonVisibility();
  const log = el("log");
  const prevHeight = log.scrollHeight;
  const prevTop = log.scrollTop;
  try {
    const events = await fetchEventsPage({ before_id: state.oldestEventId });
    state.hasMoreOlder = events.length >= LOG_PAGE_SIZE;
    if (events.length) {
      // events are ascending by id (oldest → newer within this older page).
      const marker = state.logNewestFirst ? null : log.firstChild;
      const toInsert = state.logNewestFirst ? events.slice().reverse() : events;
      for (const event of toInsert) {
        const line = createLogLine(event);
        if (!line) continue;
        if (state.oldestEventId === 0 || event.id < state.oldestEventId) {
          state.oldestEventId = event.id;
        }
        state.lastEventId = Math.max(state.lastEventId, event.id);
        if (state.logNewestFirst) {
          log.appendChild(line);
        } else {
          log.insertBefore(line, marker);
        }
      }
      trimLogWindow(false);
      if (!state.logNewestFirst) {
        log.scrollTop = log.scrollHeight - prevHeight + prevTop;
      }
    }
  } finally {
    state.loadingOlder = false;
    updateJumpButtonVisibility();
  }
}

function scheduleLogReload() {
  if (state.logSearchTimer) clearTimeout(state.logSearchTimer);
  state.logSearchTimer = setTimeout(() => {
    state.logSearchTimer = null;
    reloadLogNewestPage().catch(() => {});
  }, 250);
}

async function selectRun(runId) {
  state.selectedRunId = runId;
  state.lastEventId = 0;
  state.oldestEventId = 0;
  state.hasMoreOlder = false;
  state.logTailPinned = true;
  state.logNewestFirst = true;
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

  await reloadLogNewestPage();
}

// -- websocket --------------------------------------------------------------

async function fetchMissedEventsForSelectedRun() {
  const runId = state.selectedRunId;
  if (!runId || state.levels.size === 0) return;

  const events = await fetchEventsPage({ after_id: state.lastEventId });
  const fresh = events.filter((e) => e.id > state.lastEventId && eventMatchesActiveFilters(e));
  if (fresh.length) {
    renderEvents(fresh, {});
  }

  const runResp = await fetch(`/api/runs/${encodeURIComponent(runId)}`);
  const runData = await runResp.json();
  if (runData.run) {
    upsertRun(runData.run);
    renderHeader(runData.run);
  }
  if (state.logTailPinned) {
    const log = el("log");
    log.scrollTop = state.logNewestFirst ? 0 : log.scrollHeight;
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
        if (state.logTailPinned && eventMatchesActiveFilters(event)) {
          appendLogLine(event, {});
        }
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

function downloadLogs() {
  const runId = state.selectedRunId;
  if (!runId) return;
  const params = new URLSearchParams();
  const types = checkedTypesParam();
  if (types) params.set("types", types);
  if (state.logSearch) params.set("q", state.logSearch);
  const qs = params.toString();
  const url = `/api/runs/${encodeURIComponent(runId)}/events/export${qs ? `?${qs}` : ""}`;
  const a = document.createElement("a");
  a.href = url;
  a.download = `nornir-run-${runId}.log`;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

async function init() {
  el("runfilter").addEventListener("input", (e) => {
    state.runFilter = e.target.value;
    requestRenderRunList();
  });
  el("logsearch").addEventListener("input", (e) => {
    state.logSearch = e.target.value.trim().toLowerCase();
    scheduleLogReload();
  });
  document.querySelectorAll(".lvl").forEach((cb) => {
    cb.addEventListener("change", () => {
      if (cb.checked) state.levels.add(cb.value);
      else state.levels.delete(cb.value);
      scheduleLogReload();
    });
  });

  const log = el("log");
  log.addEventListener("scroll", () => {
    if (state.logTailPinned) {
      const unpinned = state.logNewestFirst
        ? !isLogNearTop(log)
        : !isLogNearBottom(log);
      if (unpinned) {
        state.logTailPinned = false;
        updateJumpButtonVisibility();
      }
    }
    const atOlderEdge = state.logNewestFirst
      ? isLogNearBottom(log)
      : isLogNearTop(log);
    if (atOlderEdge) {
      loadOlderEvents().catch(() => {});
    }
  });

  el("load-older").addEventListener("click", () => {
    loadOlderEvents().catch(() => {});
  });
  el("download-logs").addEventListener("click", () => downloadLogs());

  el("jump-newest").addEventListener("click", () => {
    if (!state.logNewestFirst) {
      reverseLogOrder();
      state.logNewestFirst = true;
    }
    state.logTailPinned = true;
    reloadLogNewestPage().catch(() => {});
  });

  el("jump-oldest").addEventListener("click", () => {
    if (state.logNewestFirst) {
      reverseLogOrder();
      state.logNewestFirst = false;
      state.logTailPinned = false;
      log.scrollTop = 0;
      updateJumpButtonVisibility();
      loadOlderEvents().catch(() => {});
    }
  });

  await refreshRunList();

  connectWs();
  // Keep runtime labels fresh for live runs without rebuilding the run list.
  setInterval(tickRuntimes, 1000);
}

init();
