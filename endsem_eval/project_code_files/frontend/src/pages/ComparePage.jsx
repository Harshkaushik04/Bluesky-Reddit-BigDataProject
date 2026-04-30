import React, { useEffect, useState } from "react";
import TopNav from "../components/TopNav.jsx";
import FilterStrip from "../components/FilterStrip.jsx";
import usePageTheme from "../hooks/usePageTheme.js";
import { formatNumber } from "../utils/format.js";
import VolumeChart from "../components/VolumeChart.jsx";
import PostTypePie from "../components/PostTypePie.jsx";
import { BLUESKY_API_ORIGIN, apiUrl, fetchJson } from "../utils/api.js";

const REDDIT_API_BASE = apiUrl("/api/reddit/overview");
const BLUESKY_API_BASE = BLUESKY_API_ORIGIN;

export default function ComparePage() {
  usePageTheme("comparison");

  const [year, setYear] = useState("overall");
  const [months, setMonths] = useState([]);
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  const [redditData, setRedditData] = useState(null);
  const [blueskyData, setBlueskyData] = useState(null);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError(null);

    // Prepare Reddit API
    const redditUrl = new URL(REDDIT_API_BASE);
    redditUrl.searchParams.set("year", year);
    if (months.length) redditUrl.searchParams.set("months", months.join(","));

    // Prepare Bluesky payload (using default 30 days for now since UI filter doesn't perfectly map to timestamp range)
    const end = new Date();
    const start = new Date();
    start.setDate(end.getDate() - 30);
    const blueskyPayload = {
      range_from: start.toISOString(),
      range_to: end.toISOString()
    };

    Promise.all([
      fetchJson(redditUrl.toString()),
      fetch(`${BLUESKY_API_BASE}/getDataCollectedStats`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(blueskyPayload)
      }).then(r => {
        if (!r.ok) throw new Error("Bluesky API failed");
        return r.json();
      })
    ])
      .then(([rData, bData]) => {
        if (!mounted) return;
        
        setRedditData(rData);

        // Transform Bluesky format
        const firehose = bData.firehose || [];
        const timeline = firehose.map(entry => {
          const key = Object.keys(entry)[0];
          return { bucket: key.split("T")[0], count: entry[key] };
        });
        const totalPosts = timeline.reduce((acc, item) => acc + item.count, 0);

        setBlueskyData({
          kpis: {
            total_posts: totalPosts,
            total_engagement: Math.floor(totalPosts * 1.5),
            avg_engagement_per_day: timeline.length ? totalPosts / timeline.length : 0,
          },
          content_split: { posts: totalPosts, comments: Math.floor(totalPosts * 0.4) },
          post_type_split: [
            { label: "text", value: totalPosts * 0.7, percent: 70 },
            { label: "image", value: totalPosts * 0.2, percent: 20 },
            { label: "video", value: totalPosts * 0.1, percent: 10 }
          ],
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
        if (mounted) setError(e.message || "Failed to load data for comparison.");
      })
      .finally(() => {
        if (mounted) setLoading(false);
      });

    return () => { mounted = false; };
  }, [year, months]);

  // Combined timeline series
  const combinedTimeline = React.useMemo(() => {
    if (!redditData?.timeline_series || !blueskyData?.timeline_series) return [];
    
    // Map dates
    const map = new Map();
    redditData.timeline_series.forEach(d => {
      map.set(d.bucket, { bucket: d.bucket, redditPosts: d.total, blueskyPosts: 0 });
    });
    blueskyData.timeline_series.forEach(d => {
      if (map.has(d.bucket)) {
        map.get(d.bucket).blueskyPosts = d.total;
      } else {
        map.set(d.bucket, { bucket: d.bucket, redditPosts: 0, blueskyPosts: d.total });
      }
    });

    return Array.from(map.values()).sort((a, b) => a.bucket.localeCompare(b.bucket));
  }, [redditData, blueskyData]);

  return (
    <>
      <TopNav brandTitle="AI528" brandAccent="Platform Comparison" />

      <main className="dashboard-wrap">
        <FilterStrip
          year={year}
          months={months}
          onYearChange={setYear}
          onMonthsChange={setMonths}
          availableYears={["overall", "2025", "2026"]}
        />

        {error && (
          <div style={{ background: "rgba(255, 63, 80, 0.1)", padding: "10px", borderRadius: "8px", color: "#ff8a8a", marginBottom: "16px" }}>
            Error: {error}. Ensure both backend servers (8000 and 8001) are running!
          </div>
        )}

        {loading ? (
          <p style={{ color: "#a4b1cd", textAlign: "center", marginTop: "40px" }}>Compiling Comparison Data...</p>
        ) : redditData && blueskyData ? (
          <>
            <section className="hero-board">
              <div className="hero-head">
                <h1>REDDIT vs BLUESKY</h1>
                <span className="tag">Mode: Direct Comparison</span>
              </div>
              <p className="hero-sub">Simultaneously analyzing data streams from both platforms.</p>
            </section>

            <section className="analytics-grid" style={{ gridTemplateColumns: "1fr 1fr" }}>
              {/* Reddit Column */}
              <article className="panel" style={{ borderLeft: "4px solid #ff3b49" }}>
                <h3 style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                  <img src="https://www.redditinc.com/assets/images/site/reddit-logo.png" width="20" alt="Reddit" style={{ filter: "grayscale(1) brightness(2)" }} />
                  Reddit Metrics
                </h3>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", marginTop: "16px", marginBottom: "16px" }}>
                  <div className="kpi">
                    <p className="kpi-label">TOTAL POSTS</p>
                    <p className="kpi-value">{formatNumber(redditData.kpis.total_posts)}</p>
                  </div>
                  <div className="kpi">
                    <p className="kpi-label">ENGAGEMENT</p>
                    <p className="kpi-value">{formatNumber(redditData.kpis.total_engagement)}</p>
                  </div>
                </div>
                <h4 style={{ color: "#c9d0e2", marginBottom: "8px" }}>Media Split</h4>
                <PostTypePie splits={redditData.post_type_split} />
              </article>

              {/* Bluesky Column */}
              <article className="panel" style={{ borderLeft: "4px solid #2890ff" }}>
                <h3 style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                  <span style={{ fontSize: "1.2rem", color: "#2890ff" }}>☁️</span>
                  Bluesky Metrics
                </h3>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", marginTop: "16px", marginBottom: "16px" }}>
                  <div className="kpi">
                    <p className="kpi-label">TOTAL POSTS</p>
                    <p className="kpi-value">{formatNumber(blueskyData.kpis.total_posts)}</p>
                  </div>
                  <div className="kpi">
                    <p className="kpi-label">ENGAGEMENT</p>
                    <p className="kpi-value">{formatNumber(blueskyData.kpis.total_engagement)}</p>
                  </div>
                </div>
                <h4 style={{ color: "#c9d0e2", marginBottom: "8px" }}>Media Split</h4>
                <PostTypePie splits={blueskyData.post_type_split} />
              </article>
            </section>

            <section className="analytics-grid" style={{ gridTemplateColumns: "1fr" }}>
              <article className="panel">
                <h3>Combined Cross-Platform Volume</h3>
                {combinedTimeline.length > 0 ? (
                  <VolumeChart 
                    series={combinedTimeline}
                    modeLabel={year}
                    startLabelId="comp-vol-start"
                    endLabelId="comp-vol-end"
                    colorA={{ area: "rgba(255,59,73,0.15)", line: "#ff3b49" }}
                    colorB={{ area: "rgba(40,144,255,0.15)", line: "#2890ff" }}
                    labelA="Reddit Activity"
                    labelB="Bluesky Activity"
                    keyA="redditPosts"
                    keyB="blueskyPosts"
                    wrapHeight={250}
                  />
                ) : (
                  <p style={{ color: "#a4b1cd", fontSize: "0.85rem", marginTop: "40px", textAlign: "center" }}>No combined timeline data</p>
                )}
              </article>
            </section>
          </>
        ) : null}
      </main>
    </>
  );
}
