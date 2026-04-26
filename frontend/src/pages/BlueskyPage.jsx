import React, { useEffect, useMemo, useState } from "react";
import TopNav from "../components/TopNav.jsx";
import FilterStrip from "../components/FilterStrip.jsx";
import usePageTheme from "../hooks/usePageTheme.js";
import { fetchJson } from "../utils/api.js";
import { formatNumber } from "../utils/format.js";
import VolumeChart from "../components/VolumeChart.jsx";
import PostTypePie from "../components/PostTypePie.jsx";

const API_BASE = "http://127.0.0.1:8000/api/bluesky/overview";

export default function BlueskyPage() {
  usePageTheme("bluesky");

  const [year, setYear] = useState("overall");
  const [months, setMonths] = useState([]);
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  const apiUrl = useMemo(() => {
    const url = new URL(API_BASE);
    url.searchParams.set("year", year);
    if (months.length) url.searchParams.set("months", months.join(","));
    return url.toString();
  }, [year, months]);

  useEffect(() => {
    let mounted = true;
    setError(null);
    fetchJson(apiUrl)
      .then((json) => mounted && setData(json))
      .catch((e) => mounted && setError(e.message || "Failed to load"));
    return () => {
      mounted = false;
    };
  }, [apiUrl]);

  const kpis = data?.kpis || {};
  const split = data?.content_split || {};

  const postPct = useMemo(() => {
    const posts = Number(split.posts || 0);
    const interactions = Number(split.comments || 0);
    const total = Math.max(posts + interactions, 1);
    return Math.round((posts / total) * 100);
  }, [split.posts, split.comments]);

  return (
    <>
      <TopNav brandTitle="AI528" brandAccent="Bluesky Analytics" />

      <main className="dashboard-wrap">
        <FilterStrip year={year} months={months} onYearChange={setYear} onMonthsChange={setMonths} />

        <section className="hero-board">
          <div className="hero-head">
            <h1>BLUESKY DASHBOARD</h1>
            <span className="tag">Mode: without streaming</span>
          </div>
          <p className="hero-sub">
            {error
              ? "Could not connect to backend or no Bluesky data found."
              : data
              ? `Scanned ${formatNumber(data.meta?.records_scanned || 0)} events from Bluesky firehose files.`
              : "Loading data from backend..."}
          </p>
        </section>

        <section className="kpi-grid">
          <article className="kpi">
            <p className="kpi-label">Total Posts</p>
            <p className="kpi-value">{formatNumber(kpis.total_posts)}</p>
            <p className="kpi-note">Filtered period</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Total Engagement</p>
            <p className="kpi-value">{formatNumber(kpis.total_engagement)}</p>
            <p className="kpi-note">posts + likes + follows</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Avg Engagement (Per Day)</p>
            <p className="kpi-value">{Number(kpis.avg_engagement_per_day || 0).toFixed(2)}</p>
            <p className="kpi-note">For selected period</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Avg Sentiment</p>
            <p className="kpi-value">{Number(kpis.avg_sentiment || 0).toFixed(2)}</p>
            <p className="kpi-note">To be added later</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Avg Score</p>
            <p className="kpi-value">{Number(kpis.avg_score || 0).toFixed(2)}</p>
            <p className="kpi-note">Not in firehose posts</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Avg Comments</p>
            <p className="kpi-value">{Number(kpis.avg_comments || 0).toFixed(2)}</p>
            <p className="kpi-note">Not in firehose posts</p>
          </article>
        </section>

        <section className="analytics-grid">
          <article className="panel">
            <h3>Content Type Split</h3>
            <div
              className="donut"
              style={{
                background: `conic-gradient(var(--accent) 0 ${postPct}%, #2e3342 ${postPct}% 100%)`
              }}
            />
            <div className="legend">
              <span>Posts ({formatNumber(split.posts || 0)})</span>
              <span>Likes + Follows ({formatNumber(split.comments || 0)})</span>
            </div>
          </article>

          <article className="panel">
            <h3>Data Volume Over Time</h3>
            <VolumeChart
              series={data?.timeline_series || []}
              modeLabel={year === "overall" ? "overall" : year}
              startLabelId="bsky-volume-start"
              endLabelId="bsky-volume-end"
              labelA="Posts"
              labelB="Likes"
              labelC="Follows"
              keyA="posts"
              keyB="likes"
              keyC="follows"
              colorA={{ area: "rgba(102,178,255,0.26)", line: "#66b2ff" }}
              colorB={{ area: "rgba(59,130,246,0.22)", line: "#3b82f6" }}
              colorC={{ area: "rgba(29,78,216,0.28)", line: "#1d4ed8" }}
            />
          </article>
        </section>

        <section className="bottom-grid">
          <article className="panel">
            <h3>Top Keywords</h3>
            <div className="bar-list">
              {(data?.top_keywords || []).length ? (
                (data?.top_keywords || []).map((row) => (
                  <div key={row.label} className="bar-row">
                    <p>{row.label}</p>
                    <div className="track">
                      <div className="fill" style={{ width: "100%" }} />
                    </div>
                    <p>{formatNumber(row.value)}</p>
                  </div>
                ))
              ) : (
                <p>No data available.</p>
              )}
            </div>
          </article>

          <article className="panel">
            <h3>Post Type Distribution</h3>
            <PostTypePie split={data?.post_type_split || []} />
          </article>
        </section>
      </main>

      <div className="footer-note">Bluesky dashboard frame updated to analytics-board style.</div>
    </>
  );
}

