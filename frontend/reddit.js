const API_URL = "http://127.0.0.1:8000/api/reddit/overview";
const MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const POST_TYPE_COLORS = {
  video: "#ff8a8a",
  image: "#ff4d67",
  text: "#ff304a",
  link: "#ff6b35",
  gallery: "#f59e0b",
  poll: "#a855f7",
  crosspost: "#22c55e",
  other: "#7d3d45",
};
const FALLBACK_TYPE_COLORS = ["#f43f5e", "#f97316", "#f59e0b", "#84cc16", "#10b981", "#06b6d4", "#3b82f6", "#8b5cf6"];

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
  return dt.toLocaleDateString(undefined, { day: "2-digit", month: "short" });
}

function renderTimeline(series) {
  const container = document.getElementById("volume-chart-wrap");
  if (!container) return;
  if (!series || series.length === 0) {
    container.innerHTML = "<p>No timeline data available.</p>";
    setText("volume-start-label", "Start: -");
    setText("volume-end-label", "End: -");
    return;
  }

  const width = 760;
  const height = 165;
  const padding = { top: 8, right: 20, bottom: 18, left: 20 };
  const plotW = width - padding.left - padding.right;
  const plotH = height - padding.top - padding.bottom;

  const maxValue = Math.max(
    1,
    ...series.map((d) => Math.max(d.posts || 0, d.upvotes || 0, d.comments || 0))
  );
  const n = series.length;
  const xStep = n > 1 ? plotW / (n - 1) : plotW;
  const toPoints = (key) =>
    series.map((d, i) => ({
      x: padding.left + i * xStep,
      y: padding.top + (1 - (Number(d[key] || 0) / maxValue)) * plotH,
    }));

  const postsPts = toPoints("posts");
  const upvotesPts = toPoints("upvotes");
  const commentsPts = toPoints("comments");
  const baseY = padding.top + plotH;
  const tickIndexes = [...new Set([0, Math.floor((n - 1) / 2), Math.max(n - 1, 0)])];
  const xTicks = tickIndexes
    .map((idx) => {
      const point = postsPts[idx];
      const label = formatBucketLabel(series[idx].bucket);
      return `<text x="${point.x}" y="${height - 1}" text-anchor="middle" fill="#f7a0a8" font-size="9">${label}</text>`;
    })
    .join("");

  const grid = [0.25, 0.5, 0.75]
    .map((ratio) => {
      const y = padding.top + plotH * ratio;
      return `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="rgba(255,255,255,0.08)" stroke-width="1" />`;
    })
    .join("");

  container.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Reddit volume chart">
      ${grid}
      <path d="${buildAreaPath(commentsPts, baseY)}" fill="rgba(125,61,69,0.3)"></path>
      <path d="${buildAreaPath(upvotesPts, baseY)}" fill="rgba(255,77,103,0.24)"></path>
      <path d="${buildAreaPath(postsPts, baseY)}" fill="rgba(255,138,138,0.2)"></path>
      <path d="${buildLinePath(commentsPts)}" fill="none" stroke="#7d3d45" stroke-width="1.8"></path>
      <path d="${buildLinePath(upvotesPts)}" fill="none" stroke="#ff4d67" stroke-width="1.8"></path>
      <path d="${buildLinePath(postsPts)}" fill="none" stroke="#ff8a8a" stroke-width="1.8"></path>
      ${xTicks}
    </svg>
  `;

  setText("volume-start-label", `Start: ${series[0].bucket}`);
  setText("volume-end-label", `End: ${series[series.length - 1].bucket}`);

  const svg = container.querySelector("svg");
  if (!svg) return;
  let tooltip = container.querySelector(".chart-tooltip");
  if (!tooltip) {
    tooltip = document.createElement("div");
    tooltip.className = "chart-tooltip";
    tooltip.style.display = "none";
    container.appendChild(tooltip);
  }

  const updateTooltip = (clientX, clientY) => {
    const rect = svg.getBoundingClientRect();
    const relativeX = Math.max(0, Math.min(clientX - rect.left, rect.width));
    const normalized = rect.width <= 0 ? 0 : relativeX / rect.width;
    const idx = Math.max(0, Math.min(series.length - 1, Math.round(normalized * (series.length - 1))));
    const point = series[idx];

    tooltip.innerHTML = `
      <div><strong>${point.bucket}</strong></div>
      <div>Posts: ${formatNumber(point.posts)}</div>
      <div>Upvotes: ${formatNumber(point.upvotes)}</div>
      <div>Comments: ${formatNumber(point.comments)}</div>
      <div>Total: ${formatNumber(point.total)}</div>
    `;
    tooltip.style.display = "block";
    tooltip.style.left = `${Math.min(Math.max(relativeX + 12, 8), rect.width - 150)}px`;
    tooltip.style.top = `${Math.max((clientY - rect.top) - 76, 8)}px`;
  };

  svg.onmousemove = (event) => updateTooltip(event.clientX, event.clientY);
  svg.onmouseenter = (event) => updateTooltip(event.clientX, event.clientY);
  svg.onmouseleave = () => {
    tooltip.style.display = "none";
  };
}

function renderPostTypePie(split) {
  const pie = document.getElementById("post-type-pie");
  const list = document.getElementById("post-type-list");
  if (!pie || !list) return;

  if (!split || split.length === 0) {
    pie.style.background = "conic-gradient(#4b5563 0 100%)";
    list.innerHTML = "<p>No post type data.</p>";
    return;
  }

  let start = 0;
  const getColor = (label, idx) => POST_TYPE_COLORS[label] || FALLBACK_TYPE_COLORS[idx % FALLBACK_TYPE_COLORS.length];
  const segments = split.map((item, idx) => {
    const pct = Number(item.percent || 0);
    const end = start + pct;
    const color = getColor(item.label, idx);
    const segment = `${color} ${start}% ${end}%`;
    start = end;
    return segment;
  });
  pie.style.background = `conic-gradient(${segments.join(", ")})`;

  list.innerHTML = split
    .map((item, idx) => {
      const color = getColor(item.label, idx);
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
      await loadRedditDashboard();
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
      await loadRedditDashboard();
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

async function loadRedditDashboard() {
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
    const comments = data.content_split?.comments || 0;
    const total = Math.max(posts + comments, 1);
    const postPct = Math.round((posts / total) * 100);
    const donut = document.getElementById("content-split-donut");
    if (donut) donut.style.background = `conic-gradient(var(--accent) 0 ${postPct}%, #2e3342 ${postPct}% 100%)`;
    setText("legend-posts", `Posts (${formatNumber(posts)})`);
    setText("legend-comments", `Comments (${formatNumber(comments)})`);

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
      `Scanned ${formatNumber(data.meta?.records_scanned || 0)} posts. Year=${filterState.year}, Months=${filterState.months.length ? filterState.months.join(",") : "all"}.`
    );
    setText("data-mode-tag", "Mode: Real-time");
  } catch (error) {
    setText("scan-info", "Could not connect to backend or no data for selected filters.");
    console.error("Failed to load Reddit dashboard:", error);
  }
}

setupMonthButtons();
setupFilterEvents();
refreshButtonStates();
loadRedditDashboard();
