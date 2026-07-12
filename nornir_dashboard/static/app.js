"use strict";

const state = {
  runs: new Map(),       // run_id -> summary
  selectedRunId: null,
  lastEventId: 0,        // highest event id rendered for the selected run
  runFilter: "",
  logSearch: "",
  levels: new Set(["error", "warning", "info", "event", "status"]),
};

const el = (id) => document.getElementById(id);

function fmtTime(ts) {
  if (!ts) return "";
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString();
}

function statusClass(status) {
  switch (status) {
    case "completed": return "completed";
    case "failed": return "failed";
    case "skipped": return "skipped";
    default: return "running";
  }
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
    li.innerHTML = `
      <div class="r-line1">
        <span class="r-pipeline">${escapeHtml(run.pipeline || "(pipeline)")}</span>
        <span class="badge ${statusClass(status)}">${escapeHtml(status)}</span>
      </div>
      <div class="r-volume" title="${escapeHtml(run.volumepath || "")}">${escapeHtml(run.volumepath || "")}</div>
      <div class="r-counts">
        <span>${escapeHtml(run.run_id)}</span>
        &middot; <span class="err">${run.error_count || 0} err</span>
        &middot; <span class="warn">${run.warning_count || 0} warn</span>
      </div>`;
    ul.appendChild(li);
  }
}

// -- detail -----------------------------------------------------------------

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
  el("d-errors").textContent = run.error_count || 0;
  el("d-warnings").textContent = run.warning_count || 0;
  el("d-stage").textContent = run.current_stage || "-";
  el("d-section").textContent = run.current_section || run.current_element || "-";

  let fraction = run.progress_fraction;
  if ((fraction === null || fraction === undefined) && run.progress_total) {
    fraction = run.progress_current / run.progress_total;
  }
  const pct = fraction ? Math.max(0, Math.min(1, fraction)) * 100 : 0;
  el("d-progress-fill").style.width = pct.toFixed(1) + "%";

  let label = "-";
  if (run.progress_total) {
    label = `${run.progress_current || 0}/${run.progress_total} (${pct.toFixed(0)}%)`;
  } else if (fraction) {
    label = `${pct.toFixed(0)}%`;
  }
  el("d-progress-label").textContent = label;
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
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
  log.appendChild(line);
  if (atBottom) log.scrollTop = log.scrollHeight;
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

  const resp = await fetch("/api/runs");
  const data = await resp.json();
  for (const run of data.runs) upsertRun(run);
  renderRunList();

  connectWs();
}

init();
