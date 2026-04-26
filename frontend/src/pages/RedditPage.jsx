import React, { useEffect, useMemo, useState } from "react";
import TopNav from "../components/TopNav.jsx";
import FilterStrip from "../components/FilterStrip.jsx";
import usePageTheme from "../hooks/usePageTheme.js";
import { fetchJson } from "../utils/api.js";
import { formatNumber } from "../utils/format.js";
import VolumeChart from "../components/VolumeChart.jsx";
import PostTypePie from "../components/PostTypePie.jsx";

const API_BASE = "http://127.0.0.1:8000/api/reddit/overview";

export default function RedditPage() {
  usePageTheme("reddit");

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
    const comments = Number(split.comments || 0);
    const total = Math.max(posts + comments, 1);
    return Math.round((posts / total) * 100);
  }, [split.posts, split.comments]);

  return (
    <>
      <TopNav brandTitle="AI528" brandAccent="Reddit Analytics" />

      <main className="dashboard-wrap">
        <FilterStrip year={year} months={months} onYearChange={setYear} onMonthsChange={setMonths} />

        <section className="hero-board">
          <div className="hero-head">
            <h1>REDDIT DASHBOARD</h1>
            <span className="tag">Mode: without streaming</span>
          </div>
          <p className="hero-sub">
            {error
              ? "Could not connect to backend. Start FastAPI server first."
              : data
              ? `Scanned ${formatNumber(data.meta?.records_scanned || 0)} posts.`
              : "Loading data from backend..."}
          </p>
        </section>

        <section className="kpi-grid">
          <article className="kpi">
            <p className="kpi-label">Total Posts</p>
            <p className="kpi-value">{formatNumber(kpis.total_posts)}</p>
            <p className="kpi-note">Without streaming</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Total Engagement</p>
            <p className="kpi-value">{formatNumber(kpis.total_engagement)}</p>
            <p className="kpi-note">posts + upvotes + downvotes + comments</p>
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
            <p className="kpi-note">From post score</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Avg Comments</p>
            <p className="kpi-value">{Number(kpis.avg_comments || 0).toFixed(2)}</p>
            <p className="kpi-note">Per post in selection</p>
          </article>
        </section>

        <section className="analytics-grid">
          <article className="panel">
            <h3>Data Volume Over Time</h3>
            <VolumeChart
              series={data?.timeline_series || []}
              modeLabel={year === "overall" ? "overall" : year}
              startLabelId="volume-start-label"
              endLabelId="volume-end-label"
              labelA="Posts"
              labelB="Upvotes"
              labelC="Comments"
              keyA="posts"
              keyB="upvotes"
              keyC="comments"
              colorA={{ area: "rgba(255,138,138,0.2)", line: "#ff8a8a" }}
              colorB={{ area: "rgba(255,77,103,0.24)", line: "#ff4d67" }}
              colorC={{ area: "rgba(125,61,69,0.3)", line: "#7d3d45" }}
            />
          </article>

          <article className="panel">
            <h3>Content Split</h3>
            <div
              className="donut"
              style={{
                background: `conic-gradient(var(--accent) 0 ${postPct}%, #2e3342 ${postPct}% 100%)`
              }}
            />
            <div className="legend">
              <span>Posts ({formatNumber(split.posts || 0)})</span>
              <span>Comments ({formatNumber(split.comments || 0)})</span>
            </div>
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

      <div className="footer-note">Reddit dashboard frame updated to analytics-board style.</div>
    </>
  );
}

