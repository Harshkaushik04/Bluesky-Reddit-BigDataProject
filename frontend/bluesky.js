const API_URL = "http://10.116.37.242:8000/api/bluesky/overview";
const MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const POST_TYPE_COLORS = {
  video: "#66b2ff",
  photo: "#3b82f6",
  text: "#1d4ed8",
  other: "#64748b",
};

const filterState = { year: "overall", months: [] };

function formatNumber(value) {
  return new Intl.NumberFormat().format(value || 0);
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function renderBarRows(containerId, rows) {
  const container = document.getElementById(containerId);
  if (!container) return;
  if (!rows || rows.length === 0) {
    container.innerHTML = "<p>No data available.</p>";
    return;
  }
  const maxValue = Math.max(...rows.map((row) => row.value), 1);
  container.innerHTML = rows
    .map((row) => {
      const width = Math.max((row.value / maxValue) * 100, 4);
      return `<div class="bar-row"><p>${row.label}</p><div class="track"><div class="fill" style="width:${width}%"></div></div><p>${formatNumber(row.value)}</p></div>`;
    })
    .join("");
}

function buildAreaPath(points, height) {
  if (points.length === 0) return "";
  const start = `M ${points[0].x} ${height} L ${points[0].x} ${points[0].y}`;
  const mid = points.map((p) => `L ${p.x} ${p.y}`).join(" ");
  const end = `L ${points[points.length - 1].x} ${height} Z`;
  return `${start} ${mid} ${end}`;
}

function buildLinePath(points) {
  if (points.length === 0) return "";
  return `M ${points.map((p) => `${p.x} ${p.y}`).join(" L ")}`;
}

function formatBucketLabel(bucket) {
  const dt = new Date(`${bucket}T00:00:00`);
  if (Number.isNaN(dt.getTime())) return bucket;
  if (filterState.year === "overall") {
    return dt.toLocaleDateString(undefined, { month: "short", year: "2-digit" });
  }
  if (filterState.months.length === 1) {
    return dt.toLocaleDateString(undefined, { day: "2-digit", month: "short" });
  }
  return dt.toLocaleDateString(undefined, { day: "2-digit", month: "short" });
}

function renderTimeline(series) {
  const container = document.getElementById("volume-chart-wrap");
  if (!container) return;
  if (!series || series.length === 0) {
    container.innerHTML = "<p>No timeline data available.</p>";
    return;
  }

  const width = 760;
  const height = 165;
  const padding = { top: 8, right: 8, bottom: 14, left: 8 };
  const plotW = width - padding.left - padding.right;
  const plotH = height - padding.top - padding.bottom;

  const maxValue = Math.max(
    1,
    ...series.map((d) => Math.max(d.posts || 0, d.likes || 0, d.follows || 0))
  );
  const n = series.length;
  const xStep = n > 1 ? plotW / (n - 1) : plotW;
  const toPoints = (key) =>
    series.map((d, i) => ({
      x: padding.left + i * xStep,
      y: padding.top + (1 - (Number(d[key] || 0) / maxValue)) * plotH,
    }));

  const postsPts = toPoints("posts");
  const likesPts = toPoints("likes");
  const followsPts = toPoints("follows");
  const baseY = padding.top + plotH;
  const tickIndexes = [...new Set([0, Math.floor((n - 1) / 2), Math.max(n - 1, 0)])];
  const xTicks = tickIndexes
    .map((idx) => {
      const point = postsPts[idx];
      const label = formatBucketLabel(series[idx].bucket);
      return `<text x="${point.x}" y="${height - 1}" text-anchor="middle" fill="#9fb3d7" font-size="9">${label}</text>`;
    })
    .join("");

  const grid = [0.25, 0.5, 0.75].map((ratio) => {
    const y = padding.top + plotH * ratio;
    return `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="rgba(255,255,255,0.08)" stroke-width="1" />`;
  }).join("");

  container.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Bluesky volume chart">
      ${grid}
      <path d="${buildAreaPath(followsPts, baseY)}" fill="rgba(29,78,216,0.28)"></path>
      <path d="${buildAreaPath(likesPts, baseY)}" fill="rgba(59,130,246,0.22)"></path>
      <path d="${buildAreaPath(postsPts, baseY)}" fill="rgba(102,178,255,0.26)"></path>
      <path d="${buildLinePath(followsPts)}" fill="none" stroke="#1d4ed8" stroke-width="1.8"></path>
      <path d="${buildLinePath(likesPts)}" fill="none" stroke="#3b82f6" stroke-width="1.8"></path>
      <path d="${buildLinePath(postsPts)}" fill="none" stroke="#66b2ff" stroke-width="1.8"></path>
      ${xTicks}
    </svg>
  `;
}

function renderPostTypePie(split) {
  const pie = document.getElementById("post-type-pie");
  const list = document.getElementById("post-type-list");
  if (!pie || !list) return;

  if (!split || split.length === 0) {
    pie.style.background = "conic-gradient(#334155 0 100%)";
    list.innerHTML = "<p>No post type data.</p>";
    return;
  }

  const total = split.reduce((sum, item) => sum + Number(item.value || 0), 0);
  if (total <= 0) {
    pie.style.background = "conic-gradient(#334155 0 100%)";
    list.innerHTML = split
      .map(
        (item) =>
          `<div class="post-type-row"><span class="post-type-color" style="background:${POST_TYPE_COLORS[item.label] || "#64748b"}"></span><span>${item.label}</span><span>0%</span><span>0</span></div>`
      )
      .join("");
    return;
  }

  let start = 0;
  const segments = split.map((item) => {
    const pct = Number(item.percent || 0);
    const end = start + pct;
    const color = POST_TYPE_COLORS[item.label] || "#64748b";
    const segment = `${color} ${start}% ${end}%`;
    start = end;
    return segment;
  });
  pie.style.background = `conic-gradient(${segments.join(", ")})`;

  list.innerHTML = split
    .map((item) => {
      const color = POST_TYPE_COLORS[item.label] || "#64748b";
      return `<div class="post-type-row"><span class="post-type-color" style="background:${color}"></span><span>${item.label}</span><span>${Number(item.percent || 0).toFixed(1)}%</span><span>${formatNumber(item.value || 0)}</span></div>`;
    })
    .join("");
}

function refreshButtonStates() {
  document.querySelectorAll("#year-buttons .filter-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.year === filterState.year);
  });
  document.querySelectorAll("#month-buttons .filter-btn").forEach((btn) => {
    const month = Number(btn.dataset.month);
    btn.classList.toggle("active", filterState.months.includes(month));
  });
}

function setupMonthButtons() {
  const container = document.getElementById("month-buttons");
  if (!container) return;
  container.innerHTML = MONTH_LABELS.map(
    (label, idx) => `<button class="filter-btn" data-month="${idx + 1}">${label}</button>`
  ).join("");
}

function setupFilterEvents() {
  document.querySelectorAll("#year-buttons .filter-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      filterState.year = btn.dataset.year || "overall";
      refreshButtonStates();
      await loadBlueskyDashboard();
    });
  });

  document.querySelectorAll("#month-buttons .filter-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const month = Number(btn.dataset.month);
      if (filterState.months.includes(month)) {
        filterState.months = filterState.months.filter((m) => m !== month);
      } else {
        filterState.months.push(month);
        filterState.months.sort((a, b) => a - b);
      }
      refreshButtonStates();
      await loadBlueskyDashboard();
    });
  });
}

function buildApiUrl() {
  const url = new URL(API_URL);
  url.searchParams.set("year", filterState.year);
  if (filterState.months.length > 0) {
    url.searchParams.set("months", filterState.months.join(","));
  }
  return url.toString();
}

async function loadBlueskyDashboard() {
  try {
    const response = await fetch(buildApiUrl());
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const kpis = data.kpis || {};

    setText("kpi-total-posts", formatNumber(kpis.total_posts));
    setText("kpi-total-engagement", formatNumber(kpis.total_engagement));
    setText("kpi-avg-engagement-day", Number(kpis.avg_engagement_per_day || 0).toFixed(2));
    setText("kpi-avg-sentiment", Number(kpis.avg_sentiment || 0).toFixed(2));
    setText("kpi-avg-score", Number(kpis.avg_score || 0).toFixed(2));
    setText("kpi-avg-comments", Number(kpis.avg_comments || 0).toFixed(2));

    const posts = data.content_split?.posts || 0;
    const interactions = data.content_split?.comments || 0;
    const total = Math.max(posts + interactions, 1);
    const postPct = Math.round((posts / total) * 100);
    const donut = document.getElementById("content-split-donut");
    if (donut) donut.style.background = `conic-gradient(var(--accent) 0 ${postPct}%, #2e3342 ${postPct}% 100%)`;
    setText("legend-posts", `Posts (${formatNumber(posts)})`);
    setText("legend-comments", `Likes + Follows (${formatNumber(interactions)})`);

    renderBarRows("top-keywords", data.top_keywords || []);
    renderTimeline(data.timeline_series || []);
    renderPostTypePie(data.post_type_split || []);
    const periodText =
      filterState.year === "overall"
        ? "Period: overall years"
        : filterState.months.length === 0
        ? `Period: year ${filterState.year} (all months)`
        : `Period: year ${filterState.year}, months ${filterState.months.join(", ")}`;
    setText("volume-period-label", periodText);

    setText(
      "scan-info",
      `Scanned ${formatNumber(data.meta?.records_scanned || 0)} events from Bluesky firehose files.`
    );
    setText("data-mode-tag", "Mode: Real-time");
  } catch (error) {
    setText("scan-info", "Could not connect to backend or no Bluesky data found.");
    console.error("Failed to load Bluesky dashboard:", error);
  }
}

setupMonthButtons();
setupFilterEvents();
refreshButtonStates();
loadBlueskyDashboard();
