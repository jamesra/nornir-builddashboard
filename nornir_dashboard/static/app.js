"use strict";

/** Max log lines kept in the DOM (sliding window over retained history). */
const LOG_DOM_MAX = 5000;
/** Events fetched per history page (API clamps to 5000). */
const LOG_PAGE_SIZE = 2000;
/** Paint at most this many live log lines per flush; beyond that, catch up via API. */
const LIVE_PAINT_MAX = 200;
/** Remaining buffered WS items that force catch-up mode instead of per-line paint. */
const LIVE_BACKLOG_SKIP = 400;

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
  /** Last header snapshot string for the selected run; skip DOM rebuild when unchanged. */
  lastHeaderSnapshot: null,
  showPools: true,
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

function filteredSortedRuns() {
  const filter = state.runFilter.toLowerCase();
  const runs = [];
  for (const run of Array.from(state.runs.values()).sort(compareRuns)) {
    if (!run.pipeline || !String(run.pipeline).trim()) continue;
    const hay = `${run.pipeline || ""} ${run.volumepath || ""} ${run.run_id}`.toLowerCase();
    if (filter && !hay.includes(filter)) continue;
    runs.push(run);
  }
  return runs;
}

/** Fields shown on a sidebar card (excludes ticking runtime text). */
function sidebarCardSnapshot(run) {
  if (!run) return "";
  const progress = sidebarProgressDisplay(run);
  return JSON.stringify({
    run_id: run.run_id || null,
    pipeline: run.pipeline || null,
    status: run.status || null,
    volumepath: run.volumepath || null,
    start_ts: run.start_ts || run.first_seen || null,
    error_count: run.error_count || 0,
    warning_count: run.warning_count || 0,
    selected: run.run_id === state.selectedRunId,
    progress_showBar: !!progress.showBar,
    progress_pct: progress.pct || 0,
    progress_label: progress.label || "",
  });
}

function runListItemHtml(run) {
  const status = run.status || "running";
  const started = run.start_ts || run.first_seen;
  const runtime = fmtDuration(runRuntimeSeconds(run));
  const sidebarProgress = sidebarProgressDisplay(run);
  const pct = sidebarProgress.pct || 0;
  const progressLabel = sidebarProgress.label || "";
  const progressBarHtml = sidebarProgress.showBar
    ? `<div class="r-progress"><div class="r-progress-fill" style="width:${pct.toFixed(1)}%"></div></div>`
    : "";

  return `
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
}

/** Patch an existing card in place when possible (avoids destroying the click target). */
function updateRunListItem(li, run) {
  const snap = sidebarCardSnapshot(run);
  const selected = run.run_id === state.selectedRunId;
  li.classList.toggle("selected", selected);
  if (li.dataset.sidebarSnap === snap) {
    return;
  }
  li.dataset.sidebarSnap = snap;
  li.innerHTML = runListItemHtml(run);
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
  const runs = filteredSortedRuns();
  const desiredIds = runs.map((r) => r.run_id);
  const existing = new Map();
  for (const li of ul.querySelectorAll(".run-item[data-run-id]")) {
    existing.set(li.dataset.runId, li);
  }

  // Drop cards no longer in the filtered list.
  for (const [runId, li] of existing) {
    if (!desiredIds.includes(runId)) {
      li.remove();
      existing.delete(runId);
    }
  }

  // Create/update and ensure DOM order matches sorted runs.
  let insertBefore = ul.firstChild;
  for (const run of runs) {
    let li = existing.get(run.run_id);
    if (!li) {
      li = document.createElement("li");
      li.className = "run-item";
      li.dataset.runId = run.run_id;
      existing.set(run.run_id, li);
    }
    updateRunListItem(li, run);
    if (li !== insertBefore) {
      ul.insertBefore(li, insertBefore);
    }
    insertBefore = li.nextSibling;
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
    state.lastHeaderSnapshot = null;
    el("detail").classList.add("hidden");
    el("detail-empty").classList.remove("hidden");
    el("log").innerHTML = "";
    updateDownloadLogsEnabled();
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

function poolTracksList(run) {
  if (!run.pool_tracks || typeof run.pool_tracks !== "object") return [];
  return Object.entries(run.pool_tracks).map(([name, track]) => {
    const row = (track && typeof track === "object") ? { ...track } : {};
    if (!row.label) row.label = name;
    row._key = name;
    return row;
  });
}

function renderPoolTracks(run) {
  const section = el("d-pools-section");
  const container = el("d-pool-tracks");
  if (!section || !container) return;
  container.innerHTML = "";

  if (!state.showPools || isTerminalStatus(run && run.status)) {
    section.classList.add("hidden");
    return;
  }

  const tracks = poolTracksList(run).filter((t) => (t.outstanding || 0) > 0 || (t.queued || 0) > 0 || (t.active || 0) > 0);
  if (!tracks.length) {
    section.classList.add("hidden");
    return;
  }
  section.classList.remove("hidden");

  const maxOutstanding = Math.max(
    1,
    ...tracks.map((t) => Number(t.outstanding) || ((Number(t.queued) || 0) + (Number(t.active) || 0))),
  );

  const sorted = tracks.slice().sort((a, b) => String(a.label || "").localeCompare(String(b.label || "")));
  for (const track of sorted) {
    const queued = Number(track.queued) || 0;
    const active = track.active == null ? null : Number(track.active) || 0;
    const outstanding = Number(track.outstanding);
    const total = Number.isFinite(outstanding)
      ? outstanding
      : queued + (active == null ? 0 : active);
    const basePct = Math.max(0, Math.min(100, (total / maxOutstanding) * 100));
    const activePct = (active != null && total > 0)
      ? Math.max(0, Math.min(100, (active / total) * basePct))
      : 0;
    const tipParts = [`outstanding ${total}`];
    if (track.queued != null) tipParts.push(`queued ${queued}`);
    if (active != null) tipParts.push(`active ${active}`);
    if (track.max_workers != null) tipParts.push(`workers ${track.max_workers}`);

    const row = document.createElement("div");
    row.className = "pool-track";
    row.title = tipParts.join(" · ");
    row.innerHTML =
      `<span class="pool-track-name">${escapeHtml(track.label || track._key || "pool")}</span>` +
      `<div class="pool-bar">` +
      `<div class="pool-fill-base" style="width:${basePct.toFixed(1)}%"></div>` +
      (active != null
        ? `<div class="pool-fill-active" style="width:${activePct.toFixed(1)}%"></div>`
        : "") +
      `</div>` +
      `<span class="pool-track-count">${total}</span>`;
    container.appendChild(row);
  }
}

function headerSnapshot(run) {
  /** Stable string of header/progress fields (excludes runtime; tickRuntimes owns that). */
  if (!run) return "";
  return JSON.stringify({
    run_id: run.run_id || null,
    pipeline: run.pipeline || null,
    status: run.status || null,
    volumepath: run.volumepath || null,
    host: run.host || null,
    compute: run.compute || null,
    error_count: run.error_count || 0,
    warning_count: run.warning_count || 0,
    current_stage: run.current_stage || null,
    current_section: run.current_section || null,
    current_element: run.current_element || null,
    current_path: run.current_path || null,
    progress_current: run.progress_current ?? null,
    progress_total: run.progress_total ?? null,
    progress_fraction: run.progress_fraction ?? null,
    progress_tracks: run.progress_tracks || {},
    pool_tracks: run.pool_tracks || {},
    showPools: state.showPools,
  });
}

function renderHeader(run, options) {
  if (!run) return;
  const force = options && options.force;
  const snap = headerSnapshot(run);
  if (!force && snap === state.lastHeaderSnapshot) {
    return;
  }
  state.lastHeaderSnapshot = snap;

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
  renderPoolTracks(run);
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
  if (event.kind === "log") {
    const level = (event.level || "info").toString().trim().toLowerCase();
    return level || "info";
  }
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
  syncLevelsFromCheckboxes();
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
  // Always trust the checkbox DOM: browsers can restore form state without
  // firing ``change``, which would leave ``state.levels`` stale for live WS inserts.
  syncLevelsFromCheckboxes();
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
  // Enforce checkbox/search filters on every insert (API filter is not enough alone).
  if (!eventMatchesActiveFilters(event)) return null;
  const key = logFilterKey(event);

  const line = document.createElement("div");
  line.className = "log-line " + (event.level || event.kind || "");
  line.dataset.key = key;
  line.dataset.id = String(event.id);
  line.dataset.text = formatEvent(event).toLowerCase();

  const tag = event.level || (event.kind === "event" ? "event" : event.kind);
  const lineNo = event.id != null ? String(event.id) : "";
  line.innerHTML =
    `<span class="n">${escapeHtml(lineNo)}</span>` +
    `<span class="t">${fmtTime(event.ts)}</span>` +
    `<span class="k">${escapeHtml(tag || "")}</span>` +
    `<span class="m">${escapeHtml(formatEvent(event))}</span>`;
  return line;
}

function syncLevelsFromCheckboxes() {
  /** Rebuild ``state.levels`` from the log-level checkboxes. */
  state.levels = new Set(
    Array.from(document.querySelectorAll(".lvl:checked")).map((cb) =>
      String(cb.value || "").trim().toLowerCase()
    ).filter(Boolean),
  );
}

function enableLogLevel(level) {
  /** Show only the matching log level: check it, uncheck the others, reload. */
  const normalized = String(level || "").trim().toLowerCase();
  document.querySelectorAll(".lvl").forEach((cb) => {
    cb.checked = String(cb.value || "").trim().toLowerCase() === normalized;
  });
  syncLevelsFromCheckboxes();
  pruneLogDomToActiveFilters();
  updateDownloadLogsEnabled();
  scheduleLogReload();
}

function pruneLogDomToActiveFilters() {
  /** Drop DOM lines that no longer match the active level/search filters. */
  const log = el("log");
  if (!log) return;
  const toRemove = [];
  for (const child of log.children) {
    const key = child.dataset.key;
    if (!key || !state.levels.has(key)) {
      toRemove.push(child);
      continue;
    }
    if (state.logSearch) {
      const text = child.dataset.text || "";
      if (!text.includes(state.logSearch)) toRemove.push(child);
    }
  }
  for (const node of toRemove) node.remove();
  syncWindowIdsFromDom();
  updateJumpButtonVisibility();
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
  const log = el("log");
  const fragment = document.createDocumentFragment();
  const lines = [];
  for (const event of events) {
    state.lastEventId = Math.max(state.lastEventId, event.id);
    if (state.oldestEventId === 0 || event.id < state.oldestEventId) {
      state.oldestEventId = event.id;
    }
    const line = createLogLine(event);
    if (line) lines.push(line);
  }
  if (!lines.length) {
    updateJumpButtonVisibility();
    return;
  }

  // Batch DOM inserts; newest-first live tail inserts at the top as a block.
  if (state.logNewestFirst && !opts.atOldestEnd) {
    for (let i = lines.length - 1; i >= 0; i--) {
      fragment.appendChild(lines[i]);
    }
    log.insertBefore(fragment, log.firstChild);
  } else if (!state.logNewestFirst && opts.atOldestEnd) {
    for (let i = lines.length - 1; i >= 0; i--) {
      fragment.appendChild(lines[i]);
    }
    log.insertBefore(fragment, log.firstChild);
  } else if (state.logNewestFirst && opts.atOldestEnd) {
    for (const line of lines) fragment.appendChild(line);
    log.appendChild(fragment);
  } else {
    for (const line of lines) fragment.appendChild(line);
    log.appendChild(fragment);
  }

  trimLogWindow(state.logTailPinned || !opts.atOldestEnd);
  if (state.logTailPinned && !opts.atOldestEnd) {
    log.scrollTop = state.logNewestFirst ? 0 : log.scrollHeight;
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
  state.lastHeaderSnapshot = null;
  el("detail-empty").classList.add("hidden");
  el("detail").classList.remove("hidden");
  el("log").innerHTML = "";
  updateDownloadLogsEnabled();
  renderRunList();

  const runResp = await fetch(`/api/runs/${encodeURIComponent(runId)}`);
  const runData = await runResp.json();
  if (runData.run) {
    upsertRun(runData.run);
    renderHeader(runData.run, { force: true });
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

/** Buffered live WS events; flushed in batches bounded by a UTC receive cutoff. */
const liveBuffer = [];
let liveFlushScheduled = false;
let liveCatchupNeeded = false;

function scheduleLiveFlush() {
  if (liveFlushScheduled) return;
  liveFlushScheduled = true;
  requestAnimationFrame(flushLiveBatch);
}

function collectLiveItem(data, latestRunById, selectedEvents) {
  /** Fold a WS frame into run/event accumulators for the current flush. */
  if (data.type === "event_batch") {
    for (const run of data.runs || []) {
      if (run && run.run_id) latestRunById.set(run.run_id, run);
    }
    for (const event of data.events || []) {
      if (event && event.id > 0
          && event.run_id === state.selectedRunId && event.id > state.lastEventId) {
        selectedEvents.push(event);
      }
    }
    return;
  }
  if (data.type !== "event") return;
  if (data.run && data.run.run_id) {
    latestRunById.set(data.run.run_id, data.run);
  }
  const event = data.event;
  if (event && event.id > 0
      && event.run_id === state.selectedRunId && event.id > state.lastEventId) {
    selectedEvents.push(event);
  }
}

function flushLiveBatch() {
  liveFlushScheduled = false;
  const cutoff = Date.now() / 1000;
  const batch = [];
  while (liveBuffer.length && liveBuffer[0].receivedAt <= cutoff) {
    batch.push(liveBuffer.shift());
  }
  if (!batch.length) {
    if (liveBuffer.length) scheduleLiveFlush();
    return;
  }

  const latestRunById = new Map();
  const selectedEvents = [];
  for (const item of batch) {
    collectLiveItem(item.data, latestRunById, selectedEvents);
  }

  for (const run of latestRunById.values()) {
    upsertRun(run);
  }
  if (latestRunById.size) {
    requestRenderRunList();
  }

  if (state.selectedRunId) {
    const selectedRun = latestRunById.get(state.selectedRunId)
      || state.runs.get(state.selectedRunId);
    if (selectedRun) {
      renderHeader(selectedRun);
    }
    if (selectedEvents.length) {
      selectedEvents.sort((a, b) => a.id - b.id);
      const backlogHeavy =
        selectedEvents.length > LIVE_PAINT_MAX || liveBuffer.length > LIVE_BACKLOG_SKIP;
      if (backlogHeavy && state.logTailPinned) {
        // Skip painting thousands of lines; advance cursor and reload when quiet.
        for (const event of selectedEvents) {
          state.lastEventId = Math.max(state.lastEventId, event.id);
        }
        liveCatchupNeeded = true;
      } else if (state.logTailPinned) {
        renderEvents(selectedEvents, {});
      } else {
        for (const event of selectedEvents) {
          state.lastEventId = Math.max(state.lastEventId, event.id);
        }
      }
    }
  }

  if (liveBuffer.length) {
    scheduleLiveFlush();
  } else if (liveCatchupNeeded) {
    liveCatchupNeeded = false;
    reloadLogNewestPage().catch(() => {});
  }
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

    if (data.type !== "event" && data.type !== "event_batch") return;

    liveBuffer.push({ data, receivedAt: Date.now() / 1000 });
    scheduleLiveFlush();
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

async function downloadLogs() {
  /** Fetch the filtered transcript as a blob and trigger a file download. */
  const runId = state.selectedRunId;
  if (!runId || state.levels.size === 0) return;
  const params = new URLSearchParams();
  const types = checkedTypesParam();
  if (types) params.set("types", types);
  if (state.logSearch) params.set("q", state.logSearch);
  const qs = params.toString();
  const url = `/api/runs/${encodeURIComponent(runId)}/events/export${qs ? `?${qs}` : ""}`;
  const filename = `nornir-run-${runId}.log`;
  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`export failed: ${resp.status}`);
    const blob = await resp.blob();
    const objectUrl = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = objectUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(objectUrl);
  } catch (err) {
    console.error("Download logs failed", err);
  }
}

function updateDownloadLogsEnabled() {
  const btn = el("download-logs");
  if (!btn) return;
  btn.disabled = !state.selectedRunId || state.levels.size === 0;
}

async function init() {
  const runsList = el("runs");
  runsList.addEventListener("pointerdown", (e) => {
    if (e.target.closest(".run-delete")) return;
    const item = e.target.closest(".run-item");
    if (!item) return;
    const runId = item.dataset.runId;
    if (runId) void selectRun(runId);
  });
  runsList.addEventListener("click", (e) => {
    const btn = e.target.closest(".run-delete");
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    const item = btn.closest(".run-item");
    if (!item) return;
    const run = state.runs.get(item.dataset.runId);
    if (run) void deleteRun(run);
  });

  el("runfilter").addEventListener("input", (e) => {
    state.runFilter = e.target.value;
    requestRenderRunList();
  });
  el("logsearch").addEventListener("input", (e) => {
    state.logSearch = e.target.value.trim().toLowerCase();
    pruneLogDomToActiveFilters();
    scheduleLogReload();
  });
  document.querySelectorAll(".lvl").forEach((cb) => {
    cb.addEventListener("change", () => {
      syncLevelsFromCheckboxes();
      pruneLogDomToActiveFilters();
      updateDownloadLogsEnabled();
      scheduleLogReload();
    });
  });
  el("d-errors").addEventListener("click", () => enableLogLevel("error"));
  el("d-warnings").addEventListener("click", () => enableLogLevel("warning"));
  syncLevelsFromCheckboxes();
  updateDownloadLogsEnabled();

  const showPools = el("show-pools");
  if (showPools) {
    state.showPools = !!showPools.checked;
    showPools.addEventListener("change", () => {
      state.showPools = !!showPools.checked;
      state.lastHeaderSnapshot = null;
      const run = state.selectedRunId ? state.runs.get(state.selectedRunId) : null;
      if (run) renderHeader(run, { force: true });
    });
  }

  // Browser form restoration (and bfcache) can change checkboxes without ``change``.
  window.addEventListener("pageshow", () => {
    syncLevelsFromCheckboxes();
    pruneLogDomToActiveFilters();
    updateDownloadLogsEnabled();
    const poolsCb = el("show-pools");
    if (poolsCb) state.showPools = !!poolsCb.checked;
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
  el("download-logs").addEventListener("click", () => {
    downloadLogs().catch(() => {});
  });

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
