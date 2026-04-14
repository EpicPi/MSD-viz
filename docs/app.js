import * as d3 from "https://cdn.jsdelivr.net/npm/d3@7/+esm";
import * as topojson from "https://cdn.jsdelivr.net/npm/topojson-client@3/+esm";

const WORLD_URL = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";
const DATA_URL = "data/sales.json";

const MS_PER_DAY = 86400000;
const MS_PER_WEEK = 7 * MS_PER_DAY;

/* ---------- state ---------- */
const state = {
  projection: "globe",
  mode: "accumulate",
  playing: false,
  weekIdx: 0,
  speedMult: 3,
  windowStart: 0,
  windowEnd: 0,
};

/* ---------- DOM ---------- */
const svg = d3.select("#map");
const gRoot = svg.append("g").attr("class", "root");
const gMap = gRoot.append("g").attr("class", "map-layer");
const gDots = gRoot.append("g").attr("class", "dots-layer");
const scrub = document.getElementById("scrub");
const dateLabel = document.getElementById("date-label");
const playBtn = document.getElementById("play");
const speedSel = document.getElementById("speed");
const projCtl = document.getElementById("projection");
const modeCtl = document.getElementById("mode");
const storefront = document.getElementById("storefront");
const storefrontCount = document.getElementById("storefront-count");
const statsEl = document.getElementById("stats");
const rangeStart = document.getElementById("range-start");
const rangeEnd = document.getElementById("range-end");
const rangeFill = document.getElementById("range-fill");
const windowLabel = document.getElementById("window-label");
const yearPresets = document.getElementById("year-presets");

/* ---------- projections ---------- */
const orthographic = d3.geoOrthographic().precision(0.3);
const mercator = d3.geoMercator();

function activeProjection() {
  return state.projection === "globe" ? orthographic : mercator;
}

function fitProjections(width, height) {
  const sphere = { type: "Sphere" };
  const currentRotate = orthographic.rotate();
  const rotated = currentRotate[0] === 0 && currentRotate[1] === 0
    ? [95, -35] // initial: center on North America
    : currentRotate;
  orthographic
    .rotate(rotated)
    .fitExtent([[20, 70], [width - 20, height - 20]], sphere);
  mercator.fitExtent(
    [[10, 50], [width - 10, height - 40]],
    sphere
  );
}

/* ---------- layout ---------- */
function currentSize() {
  const stage = document.getElementById("stage");
  return { width: stage.clientWidth, height: stage.clientHeight };
}

function applySvgSize() {
  const { width, height } = currentSize();
  svg.attr("width", width).attr("height", height).attr("viewBox", `0 0 ${width} ${height}`);
  fitProjections(width, height);
}

/* ---------- base map ---------- */
let world = null;

async function loadWorld() {
  const topo = await fetch(WORLD_URL).then((r) => r.json());
  world = {
    land: topojson.feature(topo, topo.objects.land),
    borders: topojson.mesh(topo, topo.objects.countries, (a, b) => a !== b),
    sphere: { type: "Sphere" },
    graticule: d3.geoGraticule10(),
  };
}

function drawBaseMap() {
  const path = d3.geoPath(activeProjection());
  gMap.selectAll("*").remove();
  gMap.append("path").datum(world.sphere).attr("class", "sphere").attr("d", path);
  gMap.append("path").datum(world.graticule).attr("class", "graticule").attr("d", path);
  gMap.append("path").datum(world.land).attr("class", "land").attr("d", path);
  gMap.append("path").datum(world.borders).attr("class", "borders").attr("d", path);
}

/* ---------- data ---------- */
let data = null;
let weekArrivals = []; // weekArrivals[weekIdx] = [{locIdx, n}, ...]
let weekInPerson = []; // count of in-person sales in each week
let nWeeks = 0;
let firstMonday = null;

function toDate(s) {
  // ISO YYYY-MM-DD → UTC midnight
  return new Date(s + "T00:00:00Z");
}

function weekIndexFor(date) {
  return Math.floor((date - firstMonday) / MS_PER_WEEK);
}

function dateForWeek(idx) {
  return new Date(firstMonday.getTime() + idx * MS_PER_WEEK);
}

function prepareData(raw) {
  const allDates = [];
  for (const loc of raw.locations) for (const d of loc.dates) allDates.push(d);
  for (const d of raw.in_person.dates) allDates.push(d);
  allDates.sort();

  const first = toDate(allDates[0]);
  const last = toDate(allDates[allDates.length - 1]);
  // Align first to Monday UTC
  const dow = first.getUTCDay() || 7; // 1..7, Monday=1
  firstMonday = new Date(first.getTime() - (dow - 1) * MS_PER_DAY);
  nWeeks = weekIndexFor(last) + 1;

  weekArrivals = Array.from({ length: nWeeks }, () => []);
  weekInPerson = new Array(nWeeks).fill(0);

  raw.locations.forEach((loc, locIdx) => {
    const perWeek = new Map();
    for (const ds of loc.dates) {
      const w = weekIndexFor(toDate(ds));
      perWeek.set(w, (perWeek.get(w) || 0) + 1);
    }
    for (const [w, n] of perWeek) {
      weekArrivals[w].push({ locIdx, n });
    }
  });

  for (const ds of raw.in_person.dates) {
    const w = weekIndexFor(toDate(ds));
    if (w >= 0 && w < nWeeks) weekInPerson[w] += 1;
  }
}

/* ---------- dot rendering ---------- */
function radius(count) {
  return Math.min(18, 1.8 + Math.sqrt(count) * 1.6);
}

function projectPoint(lat, lon) {
  const p = activeProjection()([lon, lat]);
  if (!p || !Number.isFinite(p[0]) || !Number.isFinite(p[1])) return null;
  return p;
}

/* --- accumulation mode --- */

// cumulative count by locIdx; recomputed on scrub or mode switch
let runningCounts = null;
let runningUpTo = -1; // the last weekIdx incorporated into runningCounts

function ensureRunning() {
  if (!runningCounts) runningCounts = new Array(data.locations.length).fill(0);
}

function runningRebuild(targetWeek) {
  ensureRunning();
  runningCounts.fill(0);
  runningUpTo = state.windowStart - 1;
  runningAdvanceTo(targetWeek);
}

function runningAdvanceTo(targetWeek) {
  ensureRunning();
  for (let w = runningUpTo + 1; w <= targetWeek; w++) {
    const arrivals = weekArrivals[w];
    for (let i = 0; i < arrivals.length; i++) {
      const { locIdx, n } = arrivals[i];
      runningCounts[locIdx] += n;
    }
  }
  runningUpTo = targetWeek;
}

function renderAccumulate() {
  ensureRunning();
  const circles = [];
  for (let i = 0; i < runningCounts.length; i++) {
    const c = runningCounts[i];
    if (c <= 0) continue;
    const loc = data.locations[i];
    const p = projectPoint(loc.lat, loc.lon);
    if (!p) continue;
    circles.push({ x: p[0], y: p[1], r: radius(c), count: c, locIdx: i });
  }

  const sel = gDots.selectAll("circle.acc").data(circles, (d) => d.locIdx);
  sel.exit().remove();
  sel
    .attr("cx", (d) => d.x)
    .attr("cy", (d) => d.y)
    .attr("r", (d) => d.r);
  sel
    .enter()
    .append("circle")
    .attr("class", "dot acc")
    .attr("cx", (d) => d.x)
    .attr("cy", (d) => d.y)
    .attr("r", (d) => d.r);
}

/* --- flash mode --- */

function renderFlashWeek(w) {
  const arrivals = weekArrivals[w] || [];
  for (const { locIdx, n } of arrivals) {
    const loc = data.locations[locIdx];
    const p = projectPoint(loc.lat, loc.lon);
    if (!p) continue;
    const rPeak = radius(n) * 1.6;
    const rEnd = radius(n) * 3.2;
    const c = gDots
      .append("circle")
      .attr("class", "dot flash")
      .attr("cx", p[0])
      .attr("cy", p[1])
      .attr("r", rPeak)
      .style("--r-peak", `${rPeak}px`)
      .style("--r-end", `${rEnd}px`);
    // Clean up DOM after animation
    setTimeout(() => c.remove(), 1500);
  }
}

function clearDots() {
  gDots.selectAll("*").remove();
}

/* --- in-person storefront --- */

function cumulativeInPerson(weekIdx) {
  let total = 0;
  for (let w = state.windowStart; w <= weekIdx; w++) total += weekInPerson[w];
  return total;
}

function updateStats() {
  const w = state.weekIdx;
  const seen = new Set();
  let sales = 0;
  for (let i = state.windowStart; i <= w; i++) {
    const arrivals = weekArrivals[i];
    for (let j = 0; j < arrivals.length; j++) {
      sales += arrivals[j].n;
      seen.add(arrivals[j].locIdx);
    }
  }
  if (statsEl) {
    statsEl.textContent = `${sales.toLocaleString()} sales · ${seen.size.toLocaleString()} locations`;
  }
}

function setStorefrontCount(total) {
  storefrontCount.textContent = total.toString();
  const scale = 1 + Math.min(0.35, total / 1200);
  storefront.style.transform = `scale(${scale.toFixed(3)})`;
}

function pulseStorefront() {
  storefront.classList.remove("pulse");
  void storefront.offsetWidth; // restart animation
  storefront.classList.add("pulse");
}

/* ---------- full re-render ---------- */

function updateDateLabel(w) {
  const d = dateForWeek(w);
  const iso = d.toISOString().slice(0, 10);
  const human = d.toLocaleString("en-US", { month: "short", year: "numeric", timeZone: "UTC" });
  dateLabel.textContent = `${human} · week of ${iso}`;
}

function render() {
  const w = state.weekIdx;

  if (state.mode === "accumulate") {
    if (w < runningUpTo) runningRebuild(w);
    else runningAdvanceTo(w);
    renderAccumulate();
  } else {
    clearDots();
    renderFlashWeek(w);
  }

  setStorefrontCount(cumulativeInPerson(w));
  updateDateLabel(w);
  updateStats();
  scrub.value = String(w);
}

/* ---------- playback ---------- */

let timer = null;

function tickInterval() {
  const base = 170; // ms per week at 1×
  return Math.max(12, Math.round(base / state.speedMult));
}

function startPlaying() {
  if (timer) return;
  if (state.weekIdx >= state.windowEnd) {
    state.weekIdx = state.windowStart;
    render();
  }
  state.playing = true;
  playBtn.textContent = "⏸";
  playBtn.classList.add("playing");
  timer = setInterval(() => {
    if (state.weekIdx >= state.windowEnd) {
      stopPlaying();
      return;
    }
    state.weekIdx += 1;
    advanceOneWeek();
  }, tickInterval());
}

function stopPlaying() {
  state.playing = false;
  playBtn.textContent = "▶";
  playBtn.classList.remove("playing");
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}

function advanceOneWeek() {
  const w = state.weekIdx;
  if (state.mode === "accumulate") {
    runningAdvanceTo(w);
    renderAccumulate();
  } else {
    renderFlashWeek(w);
  }
  const newTotal = cumulativeInPerson(w);
  const oldTotal = parseInt(storefrontCount.textContent || "0", 10) || 0;
  setStorefrontCount(newTotal);
  if (newTotal > oldTotal) pulseStorefront();
  updateDateLabel(w);
  updateStats();
  scrub.value = String(w);
}

/* ---------- wiring ---------- */

function firstWeekOfYear(year) {
  return Math.max(0, Math.min(nWeeks - 1, weekIndexFor(new Date(Date.UTC(year, 0, 1)))));
}
function lastWeekOfYear(year) {
  return Math.max(0, Math.min(nWeeks - 1, weekIndexFor(new Date(Date.UTC(year, 11, 31)))));
}

function formatWindowLabel() {
  const s = dateForWeek(state.windowStart);
  const e = dateForWeek(state.windowEnd);
  const fmt = (d) => d.toLocaleString("en-US", { month: "short", year: "numeric", timeZone: "UTC" });
  return `${fmt(s)} → ${fmt(e)}`;
}

function updateRangeDisplay() {
  rangeStart.value = String(state.windowStart);
  rangeEnd.value = String(state.windowEnd);
  const denom = Math.max(1, nWeeks - 1);
  rangeFill.style.left = `${(state.windowStart / denom) * 100}%`;
  rangeFill.style.width = `${((state.windowEnd - state.windowStart) / denom) * 100}%`;
  windowLabel.textContent = formatWindowLabel();

  // Year preset active highlight
  let activeYear = "custom";
  if (state.windowStart === 0 && state.windowEnd === nWeeks - 1) {
    activeYear = "all";
  } else {
    for (const btn of yearPresets.querySelectorAll("button")) {
      const y = btn.dataset.year;
      if (y === "all") continue;
      const yr = parseInt(y, 10);
      if (state.windowStart === firstWeekOfYear(yr) && state.windowEnd === lastWeekOfYear(yr)) {
        activeYear = y;
        break;
      }
    }
  }
  for (const btn of yearPresets.querySelectorAll("button")) {
    btn.classList.toggle("active", btn.dataset.year === activeYear);
  }
}

function setWindow(start, end, { moveScrubToStart = false } = {}) {
  start = Math.max(0, Math.min(nWeeks - 1, start));
  end = Math.max(0, Math.min(nWeeks - 1, end));
  if (end < start) [start, end] = [end, start];
  state.windowStart = start;
  state.windowEnd = end;

  scrub.min = String(start);
  scrub.max = String(end);

  if (moveScrubToStart || state.weekIdx < start || state.weekIdx > end) {
    state.weekIdx = Math.max(start, Math.min(end, state.weekIdx));
    if (moveScrubToStart) state.weekIdx = start;
  }

  if (state.playing) stopPlaying();

  // rebuild accumulation from window start
  if (state.mode === "accumulate") {
    runningRebuild(state.weekIdx);
    renderAccumulate();
  } else {
    clearDots();
    renderFlashWeek(state.weekIdx);
  }
  setStorefrontCount(cumulativeInPerson(state.weekIdx));
  updateDateLabel(state.weekIdx);
  scrub.value = String(state.weekIdx);
  updateRangeDisplay();
}

function setMode(mode) {
  if (mode === state.mode) return;
  state.mode = mode;
  for (const b of modeCtl.querySelectorAll("button")) {
    b.classList.toggle("active", b.dataset.value === mode);
  }
  clearDots();
  if (mode === "accumulate") {
    runningRebuild(state.weekIdx);
    renderAccumulate();
  }
  setStorefrontCount(cumulativeInPerson(state.weekIdx));
}

function setProjection(p) {
  if (p === state.projection) return;
  state.projection = p;
  for (const b of projCtl.querySelectorAll("button")) {
    b.classList.toggle("active", b.dataset.value === p);
  }
  resetZoom();
  drawBaseMap();
  // re-render dots in current projection
  if (state.mode === "accumulate") {
    renderAccumulate();
  } else {
    clearDots();
  }
}

const zoomBehavior = d3.zoom()
  .scaleExtent([0.5, 20])
  .filter((event) => {
    // Ignore right-click and modifier-drag. Wheel always zooms.
    // For globe, drag is reserved for rotation (handled separately).
    if (event.ctrlKey || event.button) return false;
    return event.type === "wheel" || state.projection === "mercator";
  })
  .on("zoom", (event) => {
    gRoot.attr("transform", event.transform);
  });

function resetZoom() {
  svg.call(zoomBehavior.transform, d3.zoomIdentity);
}

function wireControls() {
  svg.call(zoomBehavior).on("dblclick.zoom", null);

  playBtn.addEventListener("click", () => {
    if (state.playing) stopPlaying();
    else startPlaying();
  });

  speedSel.addEventListener("change", () => {
    state.speedMult = parseFloat(speedSel.value);
    if (state.playing) {
      stopPlaying();
      startPlaying();
    }
  });

  scrub.addEventListener("input", () => {
    const w = parseInt(scrub.value, 10);
    state.weekIdx = w;
    if (state.playing) stopPlaying();
    render();
  });

  const MIN_SPAN = 1;
  rangeStart.addEventListener("input", () => {
    let s = parseInt(rangeStart.value, 10);
    const e = parseInt(rangeEnd.value, 10);
    if (s > e - MIN_SPAN) s = e - MIN_SPAN;
    setWindow(s, e);
  });
  rangeEnd.addEventListener("input", () => {
    const s = parseInt(rangeStart.value, 10);
    let e = parseInt(rangeEnd.value, 10);
    if (e < s + MIN_SPAN) e = s + MIN_SPAN;
    setWindow(s, e);
  });

  for (const btn of yearPresets.querySelectorAll("button")) {
    btn.addEventListener("click", () => {
      const y = btn.dataset.year;
      if (y === "all") {
        setWindow(0, nWeeks - 1, { moveScrubToStart: true });
      } else {
        const yr = parseInt(y, 10);
        setWindow(firstWeekOfYear(yr), lastWeekOfYear(yr), { moveScrubToStart: true });
      }
    });
  }

  for (const btn of projCtl.querySelectorAll("button")) {
    btn.addEventListener("click", () => setProjection(btn.dataset.value));
  }
  for (const btn of modeCtl.querySelectorAll("button")) {
    btn.addEventListener("click", () => setMode(btn.dataset.value));
  }

  // drag-to-rotate the globe
  let lastPos = null;
  svg.on("mousedown", (event) => {
    if (state.projection !== "globe") return;
    lastPos = [event.clientX, event.clientY];
  });
  window.addEventListener("mousemove", (event) => {
    if (!lastPos || state.projection !== "globe") return;
    const [x0, y0] = lastPos;
    const [x1, y1] = [event.clientX, event.clientY];
    const r = orthographic.rotate();
    const k = 0.35;
    orthographic.rotate([r[0] + (x1 - x0) * k, r[1] - (y1 - y0) * k]);
    lastPos = [x1, y1];
    drawBaseMap();
    if (state.mode === "accumulate") renderAccumulate();
  });
  window.addEventListener("mouseup", () => {
    lastPos = null;
  });

  window.addEventListener("resize", () => {
    applySvgSize();
    drawBaseMap();
    if (state.mode === "accumulate") renderAccumulate();
  });
}

/* ---------- boot ---------- */

async function main() {
  const raw = await fetch(DATA_URL).then((r) => r.json());
  data = raw;
  prepareData(raw);

  state.windowStart = 0;
  state.windowEnd = nWeeks - 1;
  rangeStart.min = rangeEnd.min = "0";
  rangeStart.max = rangeEnd.max = String(nWeeks - 1);
  rangeStart.value = "0";
  rangeEnd.value = String(nWeeks - 1);
  scrub.min = "0";
  scrub.max = String(nWeeks - 1);
  scrub.value = "0";
  state.speedMult = parseFloat(speedSel.value);

  const params = new URLSearchParams(location.search);
  const parseDateParam = (s) => {
    if (!s) return null;
    const d = toDate(s);
    if (isNaN(d.getTime())) return null;
    return Math.max(0, Math.min(nWeeks - 1, weekIndexFor(d)));
  };
  const pStart = parseDateParam(params.get("start"));
  const pEnd = parseDateParam(params.get("end"));
  if (pStart !== null || pEnd !== null) {
    state.windowStart = pStart ?? 0;
    state.windowEnd = pEnd ?? nWeeks - 1;
    if (state.windowEnd < state.windowStart) state.windowEnd = state.windowStart;
    rangeStart.value = String(state.windowStart);
    rangeEnd.value = String(state.windowEnd);
    scrub.min = String(state.windowStart);
    scrub.max = String(state.windowEnd);
    state.weekIdx = state.windowStart;
  }

  if (params.has("week")) {
    state.weekIdx = Math.max(state.windowStart, Math.min(state.windowEnd, parseInt(params.get("week"), 10) || 0));
  }
  if (params.get("proj") === "mercator") {
    state.projection = "mercator";
    for (const b of projCtl.querySelectorAll("button")) {
      b.classList.toggle("active", b.dataset.value === "mercator");
    }
  }
  if (params.get("mode") === "flash") {
    state.mode = "flash";
    for (const b of modeCtl.querySelectorAll("button")) {
      b.classList.toggle("active", b.dataset.value === "flash");
    }
  }
  const initialZoom = parseFloat(params.get("zoom") || "1");
  if (initialZoom !== 1) {
    const { width, height } = currentSize();
    const cx = width / 2, cy = height / 2;
    const t = d3.zoomIdentity.translate(cx, cy).scale(initialZoom).translate(-cx, -cy);
    queueMicrotask(() => svg.call(zoomBehavior.transform, t));
  }

  applySvgSize();
  await loadWorld();
  drawBaseMap();

  wireControls();
  updateRangeDisplay();
  render();
}

main().catch((err) => {
  console.error(err);
  document.body.insertAdjacentHTML(
    "beforeend",
    `<pre style="position:fixed;top:40%;left:50%;transform:translate(-50%,-50%);color:#f66;background:#000;padding:20px;border-radius:6px;max-width:80vw;overflow:auto">Error loading visualization:\n${err.message}\n${err.stack || ""}</pre>`
  );
});
