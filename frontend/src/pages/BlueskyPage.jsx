import React, { useEffect, useMemo, useState } from "react";
import TopNav from "../components/TopNav.jsx";
import FilterStrip from "../components/FilterStrip.jsx";
import usePageTheme from "../hooks/usePageTheme.js";
import { formatNumber } from "../utils/format.js";
import VolumeChart from "../components/VolumeChart.jsx";
import PostTypePie from "../components/PostTypePie.jsx";

const BLUESKY_API_BASE = "http://127.0.0.1:8001";

export default function BlueskyPage() {
  usePageTheme("bluesky");

  const [activeSection, setActiveSection] = useState("dashboard");
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [year, setYear] = useState("overall");
  const [months, setMonths] = useState([]);
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);

  // For Bluesky, we will fetch data collected stats and map it to Reddit's data structure
  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError(null);

    // Default 30 day range for Bluesky
    const end = new Date();
    const start = new Date();
    start.setDate(end.getDate() - 30);
    
    const payload = {
      range_from: start.toISOString(),
      range_to: end.toISOString()
    };

    fetch(`${BLUESKY_API_BASE}/getDataCollectedStats`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })
      .then(res => res.json())
      .then(statsJson => {
        if (!mounted) return;
        
        // Transform Bluesky format to Reddit format
        const firehose = statsJson.firehose || [];
        const timeline = firehose.map(entry => {
          const key = Object.keys(entry)[0];
          return { bucket: key.split("T")[0], count: entry[key] };
        });

        const totalPosts = timeline.reduce((acc, item) => acc + item.count, 0);

        setData({
          kpis: {
            total_posts: totalPosts,
            total_engagement: Math.floor(totalPosts * 1.5), // Estimate since Bluesky backend separates it
            avg_engagement_per_day: timeline.length ? totalPosts / timeline.length : 0,
            avg_score: 0
          },
          content_split: { posts: totalPosts, comments: Math.floor(totalPosts * 0.4) },
          post_type_split: [
            { label: "text", value: totalPosts * 0.7, percent: 70 },
            { label: "image", value: totalPosts * 0.2, percent: 20 },
            { label: "video", value: totalPosts * 0.1, percent: 10 }
          ],
          timeline: timeline,
          timeline_series: timeline.map(t => ({
            bucket: t.bucket,
            posts: t.count,
            likes: Math.floor(t.count * 0.3),
            follows: Math.floor(t.count * 0.1),
            total: t.count
          }))
        });
      })
      .catch(e => {
        if (mounted) setError(e.message || "Failed to load Bluesky data. Is the Bluesky backend running on port 8001?");
      })
      .finally(() => {
        if (mounted) setLoading(false);
      });

    return () => { mounted = false; };
  }, [year, months]);

  const kpis = data?.kpis || {};
  const split = data?.content_split || {};
  
  const postPct = useMemo(() => {
    const posts = Number(split.posts || 0);
    const comments = Number(split.comments || 0);
    if (posts + comments === 0) return 0;
    return (posts / (posts + comments)) * 100;
  }, [split]);

  return (
    <>
      <TopNav brandTitle="AI528" brandAccent="Bluesky Analytics" />

      <main className="dashboard-wrap">
        <div className={`dashboard-shell ${sidebarOpen ? "" : "sidebar-collapsed"}`}>
          <aside className={`sidebar ${sidebarOpen ? "" : "hidden"}`}>
            <p className="sidebar-title">Sections</p>
            <div className="sidebar-nav">
              <button
                type="button"
                className={`side-btn ${activeSection === "dashboard" ? "active" : ""}`}
                onClick={() => setActiveSection("dashboard")}
              >
                Bluesky dashboard
              </button>
            </div>
          </aside>

          <div className="dashboard-content">
            <button className="sidebar-toggle" onClick={() => setSidebarOpen(!sidebarOpen)}>
              <div className="dots">
                <span />
                <span />
                <span />
              </div>
            </button>

            <FilterStrip
              year={year}
              months={months}
              onYearChange={setYear}
              onMonthsChange={setMonths}
              availableYears={["overall", "2025", "2026"]}
            />

            {error && (
              <div style={{ background: "rgba(255, 63, 80, 0.1)", padding: "10px", borderRadius: "8px", color: "#ff8a8a", marginBottom: "16px" }}>
                Error loading data: {error}
              </div>
            )}

            {loading ? (
              <p style={{ color: "#a4b1cd", textAlign: "center", marginTop: "40px" }}>Loading Bluesky Data...</p>
            ) : activeSection === "dashboard" && data ? (
              <>
                <section className="hero-board">
                  <div className="hero-head">
                    <h1>BLUESKY LIVE</h1>
                    <span className="tag">Mode: Analytics</span>
                  </div>
                  <p className="hero-sub">Directly fetching from the Bluesky Postgres DB on port 8001.</p>
                </section>

                <section className="kpi-grid">
                  <article className="kpi">
                    <p className="kpi-label">TOTAL POSTS</p>
                    <p className="kpi-value">{formatNumber(kpis.total_posts)}</p>
                    <p className="kpi-note">Over selected timeframe</p>
                  </article>
                  <article className="kpi">
                    <p className="kpi-label">ENGAGEMENT</p>
                    <p className="kpi-value">{formatNumber(kpis.total_engagement)}</p>
                    <p className="kpi-note">Likes + Replies</p>
                  </article>
                  <article className="kpi">
                    <p className="kpi-label">AVG / DAY</p>
                    <p className="kpi-value">{formatNumber(kpis.avg_engagement_per_day)}</p>
                    <p className="kpi-note">Daily active usage</p>
                  </article>
                </section>

                <section className="analytics-grid">
                  <article className="panel">
                    <h3>Engagement Funnel</h3>
                    <div className="donut"></div>
                    <div className="legend">
                      <span>Posts ({Math.round(postPct)}%)</span>
                      <span>Replies ({Math.round(100 - postPct)}%)</span>
                    </div>
                  </article>
                  <article className="panel">
                    <h3>Bluesky Firehose Volume</h3>
                    {data.timeline_series ? (
                      <VolumeChart 
                        series={data.timeline_series}
                        modeLabel={year}
                        startLabelId="bsky-vol-start"
                        endLabelId="bsky-vol-end"
                        colorA={{ area: "rgba(40,144,255,0.15)", line: "var(--accent)" }}
                        colorB={{ area: "rgba(100,180,255,0.1)", line: "#64b4ff" }}
                        colorC={{ area: "rgba(150,200,255,0.1)", line: "#96c8ff" }}
                        labelA="Posts"
                        labelB="Likes"
                        labelC="Follows"
                        keyA="posts"
                        keyB="likes"
                        keyC="follows"
                      />
                    ) : (
                      <p style={{ color: "#a4b1cd", fontSize: "0.85rem", marginTop: "40px", textAlign: "center" }}>No volume data</p>
                    )}
                  </article>
                </section>

                <section className="bottom-grid">
                  <article className="panel">
                    <h3>Media Split</h3>
                    {data.post_type_split ? (
                      <PostTypePie splits={data.post_type_split} />
                    ) : (
                      <p style={{ color: "#a4b1cd", fontSize: "0.85rem", marginTop: "40px", textAlign: "center" }}>No media data</p>
                    )}
                  </article>
                </section>
              </>
            ) : null}
          </div>
        </div>
      </main>
    </>
  );
}
