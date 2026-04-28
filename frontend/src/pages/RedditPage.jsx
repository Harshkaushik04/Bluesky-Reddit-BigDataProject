import React, { useEffect, useMemo, useState } from "react";
import TopNav from "../components/TopNav.jsx";
import FilterStrip from "../components/FilterStrip.jsx";
import usePageTheme from "../hooks/usePageTheme.js";
import { fetchJson } from "../utils/api.js";
import { formatNumber } from "../utils/format.js";
import VolumeChart from "../components/VolumeChart.jsx";
import PostTypePie from "../components/PostTypePie.jsx";

const API_BASE = "http://127.0.0.1:8000/api/reddit/overview";
const COMMENTS_API_BASE = "http://127.0.0.1:8000/api/reddit/comments/overview";
const FEATURES_API_BASE = "http://127.0.0.1:8000/api/reddit/feature-insights";

export default function RedditPage() {
  usePageTheme("reddit");

  const [activeSection, setActiveSection] = useState("dashboard"); // dashboard | comments | advanced
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [year, setYear] = useState("overall");
  const [months, setMonths] = useState([]);
  const [wordInput, setWordInput] = useState("ai");
  const [wordQuery, setWordQuery] = useState("ai");
  const [data, setData] = useState(null);
  const [commentsData, setCommentsData] = useState(null);
  const [featureData, setFeatureData] = useState(null);
  const [loadingPosts, setLoadingPosts] = useState(true);
  const [loadingComments, setLoadingComments] = useState(true);
  const [loadingFeatures, setLoadingFeatures] = useState(true);
  const [visiblePostPoints, setVisiblePostPoints] = useState(0);
  const [visibleCommentPoints, setVisibleCommentPoints] = useState(0);
  const [visibleFeaturePoints, setVisibleFeaturePoints] = useState(0);
  const [error, setError] = useState(null);

  const apiUrl = useMemo(() => {
    const url = new URL(API_BASE);
    url.searchParams.set("year", year);
    if (months.length) url.searchParams.set("months", months.join(","));
    return url.toString();
  }, [year, months]);

  const commentsApiUrl = useMemo(() => {
    const url = new URL(COMMENTS_API_BASE);
    url.searchParams.set("year", year);
    if (months.length) url.searchParams.set("months", months.join(","));
    return url.toString();
  }, [year, months]);

  const featuresApiUrl = useMemo(() => {
    const url = new URL(FEATURES_API_BASE);
    url.searchParams.set("year", year);
    if (months.length) url.searchParams.set("months", months.join(","));
    if (wordQuery.trim()) url.searchParams.set("word", wordQuery.trim().toLowerCase());
    return url.toString();
  }, [year, months, wordQuery]);

  useEffect(() => {
    let mounted = true;
    setError(null);
    setLoadingPosts(true);

    fetchJson(apiUrl)
      .then((postsJson) => {
        if (!mounted) return;
        setData(postsJson);
      })
      .catch((e) => mounted && setError((prev) => prev || e.message || "Failed to load posts data"))
      .finally(() => mounted && setLoadingPosts(false));

    return () => {
      mounted = false;
    };
  }, [apiUrl]);

  useEffect(() => {
    if (activeSection !== "comments") return;
    let mounted = true;
    setError(null);
    setLoadingComments(true);

    fetchJson(commentsApiUrl)
      .then((commentsJson) => {
        if (!mounted) return;
        setCommentsData(commentsJson);
      })
      .catch((e) => mounted && setError((prev) => prev || e.message || "Failed to load comments data"))
      .finally(() => mounted && setLoadingComments(false));

    return () => {
      mounted = false;
    };
  }, [activeSection, commentsApiUrl]);

  useEffect(() => {
    if (activeSection !== "advanced") return;
    let mounted = true;
    setError(null);
    setLoadingFeatures(true);

    fetchJson(featuresApiUrl)
      .then((featuresJson) => {
        if (!mounted) return;
        setFeatureData(featuresJson);
      })
      .catch((e) => mounted && setError((prev) => prev || e.message || "Failed to load advanced insights"))
      .finally(() => mounted && setLoadingFeatures(false));

    return () => {
      mounted = false;
    };
  }, [activeSection, featuresApiUrl]);

  const kpis = data?.kpis || {};
  const split = data?.content_split || {};
  const commentsKpis = commentsData?.kpis || {};
  const sentimentKpis = featureData?.sentiment_kpis || {};
  const controversialTopics = featureData?.controversial_topics || [];
  const trendSummary = featureData?.trend_saturation?.summary || [];
  const trendSeriesByWord = featureData?.trend_saturation?.series_by_word || {};

  const postPct = useMemo(() => {
    const posts = Number(split.posts || 0);
    const comments = Number(split.comments || 0);
    const total = Math.max(posts + comments, 1);
    return Math.round((posts / total) * 100);
  }, [split.posts, split.comments]);

  const wordKeys = useMemo(() => (featureData?.word_popularity?.top_words || []).slice(0, 3), [featureData]);
  const trendKeys = useMemo(() => (featureData?.trend_saturation?.saturation_words || []).slice(0, 3), [featureData]);
  const maxControversyScore = useMemo(
    () => Math.max(...controversialTopics.map((row) => Number(row.controversy_score || 0)), 1),
    [controversialTopics]
  );
  const postsSeries = data?.timeline_series || [];
  const commentsSeries = commentsData?.timeline_series || [];
  const featureSentimentSeries = featureData?.sentiment_timeline || [];
  const visiblePostsSeries = useMemo(() => postsSeries.slice(0, visiblePostPoints), [postsSeries, visiblePostPoints]);
  const visibleCommentsSeries = useMemo(
    () => commentsSeries.slice(0, visibleCommentPoints),
    [commentsSeries, visibleCommentPoints]
  );
  const visibleFeatureSentimentSeries = useMemo(
    () => featureSentimentSeries.slice(0, visibleFeaturePoints),
    [featureSentimentSeries, visibleFeaturePoints]
  );

  useEffect(() => {
    const total = postsSeries.length;
    if (!total) {
      setVisiblePostPoints(0);
      return;
    }
    const initial = Math.min(10, total);
    setVisiblePostPoints(initial);
    if (initial >= total) return;
    const timer = window.setInterval(() => {
      setVisiblePostPoints((prev) => {
        if (prev >= total) {
          window.clearInterval(timer);
          return prev;
        }
        const next = Math.min(prev + 12, total);
        if (next >= total) window.clearInterval(timer);
        return next;
      });
    }, 120);
    return () => window.clearInterval(timer);
  }, [postsSeries]);

  useEffect(() => {
    const total = commentsSeries.length;
    if (!total) {
      setVisibleCommentPoints(0);
      return;
    }
    const initial = Math.min(10, total);
    setVisibleCommentPoints(initial);
    if (initial >= total) return;
    const timer = window.setInterval(() => {
      setVisibleCommentPoints((prev) => {
        if (prev >= total) {
          window.clearInterval(timer);
          return prev;
        }
        const next = Math.min(prev + 12, total);
        if (next >= total) window.clearInterval(timer);
        return next;
      });
    }, 120);
    return () => window.clearInterval(timer);
  }, [commentsSeries]);

  useEffect(() => {
    const total = featureSentimentSeries.length;
    if (!total) {
      setVisibleFeaturePoints(0);
      return;
    }
    const initial = Math.min(10, total);
    setVisibleFeaturePoints(initial);
    if (initial >= total) return;
    const timer = window.setInterval(() => {
      setVisibleFeaturePoints((prev) => {
        if (prev >= total) {
          window.clearInterval(timer);
          return prev;
        }
        const next = Math.min(prev + 12, total);
        if (next >= total) window.clearInterval(timer);
        return next;
      });
    }, 120);
    return () => window.clearInterval(timer);
  }, [featureSentimentSeries]);

  return (
    <>
      <TopNav brandTitle="AI528" brandAccent="Reddit Analytics" />

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
                Reddit dashboard
              </button>
              <button
                type="button"
                className={`side-btn ${activeSection === "comments" ? "active" : ""}`}
                onClick={() => setActiveSection("comments")}
              >
                Comment analysis
              </button>
              <button
                type="button"
                className={`side-btn ${activeSection === "advanced" ? "active" : ""}`}
                onClick={() => setActiveSection("advanced")}
              >
                Advanced Features
              </button>
            </div>
          </aside>

          <div className="dashboard-content">
            <button
              type="button"
              className="sidebar-toggle"
              aria-label={sidebarOpen ? "Hide sidebar" : "Show sidebar"}
              onClick={() => setSidebarOpen((v) => !v)}
              style={{ marginBottom: "10px" }}
            >
              <span className="burger" aria-hidden="true">
                <span />
                <span />
                <span />
              </span>
            </button>

            <FilterStrip year={year} months={months} onYearChange={setYear} onMonthsChange={setMonths} />

            {activeSection === "dashboard" ? (
              <>
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
                      : loadingPosts
                      ? "Loading posts data..."
                      : "No posts data found."}
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
                    <div style={{ display: "grid", gap: "10px" }}>
                      <div>
                        <p className="volume-period-label" style={{ marginBottom: "6px" }}>Posts</p>
                        <VolumeChart
                          series={visiblePostsSeries}
                          modeLabel={year === "overall" ? "overall" : year}
                          startLabelId="posts-start-label"
                          endLabelId="posts-end-label"
                          labelA="Posts"
                          labelB=""
                          labelC=""
                          keyA="posts"
                          keyB=""
                          keyC=""
                          colorA={{ area: "rgba(255,138,138,0.2)", line: "#ff8a8a" }}
                          colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          legendColorA="#ff8a8a"
                        />
                      </div>
                      <div>
                        <p className="volume-period-label" style={{ marginBottom: "6px" }}>Upvotes</p>
                        <VolumeChart
                          series={visiblePostsSeries}
                          modeLabel={year === "overall" ? "overall" : year}
                          startLabelId="upvotes-start-label"
                          endLabelId="upvotes-end-label"
                          labelA="Upvotes"
                          labelB=""
                          labelC=""
                          keyA="upvotes"
                          keyB=""
                          keyC=""
                          colorA={{ area: "rgba(255,77,103,0.20)", line: "#ff4d67" }}
                          colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          legendColorA="#ff4d67"
                        />
                      </div>
                      <div>
                        <p className="volume-period-label" style={{ marginBottom: "6px" }}>Comments</p>
                        <VolumeChart
                          series={visiblePostsSeries}
                          modeLabel={year === "overall" ? "overall" : year}
                          startLabelId="post-comments-start-label"
                          endLabelId="post-comments-end-label"
                          labelA="Comments"
                          labelB=""
                          labelC=""
                          keyA="comments"
                          keyB=""
                          keyC=""
                          colorA={{ area: "rgba(125,61,69,0.28)", line: "#7d3d45" }}
                          colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          legendColorA="#7d3d45"
                        />
                      </div>
                    </div>
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
                    <h3>Post Type Distribution</h3>
                    <PostTypePie split={data?.post_type_split || []} />
                  </article>
                </section>
              </>
            ) : null}

            {activeSection === "comments" ? (
              <>
                <section className="hero-board" style={{ marginTop: "14px" }}>
                  <div className="hero-head">
                    <h1>REDDIT COMMENTS ANALYTICS</h1>
                    <span className="tag">From comments/*.jsonl</span>
                  </div>
                  <p className="hero-sub">
                    {loadingComments
                      ? "Loading comments data..."
                      : "Total comments, controversial comments, score behavior, and comment timeline."}
                  </p>
                </section>

                <section className="kpi-grid">
                  <article className="kpi">
                    <p className="kpi-label">Total Comments</p>
                    <p className="kpi-value">{formatNumber(commentsKpis.total_comments)}</p>
                    <p className="kpi-note">Filtered period</p>
                  </article>
                  <article className="kpi">
                    <p className="kpi-label">Controversial Comments</p>
                    <p className="kpi-value">{formatNumber(commentsKpis.total_controversial_comments)}</p>
                    <p className="kpi-note">{Number(commentsKpis.controversial_percent || 0).toFixed(2)}%</p>
                  </article>
                  <article className="kpi">
                    <p className="kpi-label">Avg Comment Upvotes</p>
                    <p className="kpi-value">{Number(commentsKpis.avg_comment_upvotes || 0).toFixed(2)}</p>
                    <p className="kpi-note">Average `ups`</p>
                  </article>
                  <article className="kpi">
                    <p className="kpi-label">Avg Comment Score</p>
                    <p className="kpi-value">{Number(commentsKpis.avg_comment_score || 0).toFixed(2)}</p>
                    <p className="kpi-note">Average `score`</p>
                  </article>
                </section>

                <section className="analytics-grid">
                  <article className="panel">
                    <h3>Comment Volume Over Time</h3>
                    <div style={{ display: "grid", gap: "10px" }}>
                      <div>
                        <p className="volume-period-label" style={{ marginBottom: "6px" }}>Total Comments</p>
                        <VolumeChart
                          series={visibleCommentsSeries}
                          modeLabel={year === "overall" ? "overall" : year}
                          startLabelId="comment-total-start"
                          endLabelId="comment-total-end"
                          labelA="Total"
                          labelB=""
                          labelC=""
                          keyA="total_comments"
                          keyB=""
                          keyC=""
                          colorA={{ area: "rgba(255,138,138,0.2)", line: "#ff8a8a" }}
                          colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          legendColorA="#ff8a8a"
                        />
                      </div>
                      <div>
                        <p className="volume-period-label" style={{ marginBottom: "6px" }}>Controversial Comments</p>
                        <VolumeChart
                          series={visibleCommentsSeries}
                          modeLabel={year === "overall" ? "overall" : year}
                          startLabelId="comment-contro-start"
                          endLabelId="comment-contro-end"
                          labelA="Controversial"
                          labelB=""
                          labelC=""
                          keyA="controversial_comments"
                          keyB=""
                          keyC=""
                          colorA={{ area: "rgba(255,77,103,0.22)", line: "#ff4d67" }}
                          colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          legendColorA="#ff4d67"
                        />
                      </div>
                      <div>
                        <p className="volume-period-label" style={{ marginBottom: "6px" }}>Non-Controversial Comments</p>
                        <VolumeChart
                          series={visibleCommentsSeries}
                          modeLabel={year === "overall" ? "overall" : year}
                          startLabelId="comment-noncontro-start"
                          endLabelId="comment-noncontro-end"
                          labelA="Non-Controversial"
                          labelB=""
                          labelC=""
                          keyA="normal_comments"
                          keyB=""
                          keyC=""
                          colorA={{ area: "rgba(125,61,69,0.28)", line: "#7d3d45" }}
                          colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          legendColorA="#7d3d45"
                        />
                      </div>
                    </div>
                  </article>

                  <article className="panel">
                    <h3>Controversial Split</h3>
                    <PostTypePie split={commentsData?.controversial_split || []} />
                  </article>
                </section>

                <section className="bottom-grid">
                  <article className="panel">
                    <h3>Comment Score Split</h3>
                    <PostTypePie split={commentsData?.score_split || []} />
                  </article>
                </section>
              </>
            ) : null}

            {activeSection === "advanced" ? (
              <>
                <section className="hero-board" style={{ marginTop: "14px" }}>
                  <div className="hero-head">
                    <h1>ADVANCED REDDIT FEATURES</h1>
                    <span className="tag">Ported from app folder feature set</span>
                  </div>
                  <p className="hero-sub">
                    {loadingFeatures
                      ? "Loading advanced insights..."
                      : "Sentiment analysis, word popularity by time, controversial topics, and trend saturation monitor."}
                  </p>
                </section>

                <section className="kpi-grid">
                  <article className="kpi">
                    <p className="kpi-label">Avg Sentiment (Titles)</p>
                    <p className="kpi-value">{Number(sentimentKpis.avg_sentiment || 0).toFixed(3)}</p>
                    <p className="kpi-note">Lexicon-based score</p>
                  </article>
                  <article className="kpi">
                    <p className="kpi-label">Positive Posts</p>
                    <p className="kpi-value">{formatNumber(sentimentKpis.positive_posts || 0)}</p>
                    <p className="kpi-note">Sentiment score {">"} 0</p>
                  </article>
                  <article className="kpi">
                    <p className="kpi-label">Negative Posts</p>
                    <p className="kpi-value">{formatNumber(sentimentKpis.negative_posts || 0)}</p>
                    <p className="kpi-note">Sentiment score {"<"} 0</p>
                  </article>
                  <article className="kpi">
                    <p className="kpi-label">Neutral Posts</p>
                    <p className="kpi-value">{formatNumber(sentimentKpis.neutral_posts || 0)}</p>
                    <p className="kpi-note">Sentiment score = 0</p>
                  </article>
                </section>

                <section className="analytics-grid">
                  <article className="panel">
                    <h3>Sentiment Analysis by Time</h3>
                    <div style={{ display: "grid", gap: "10px" }}>
                      <div>
                        <p className="volume-period-label" style={{ marginBottom: "6px" }}>Positive Posts</p>
                        <VolumeChart
                          series={visibleFeatureSentimentSeries}
                          modeLabel={year === "overall" ? "overall" : year}
                          startLabelId="sentiment-pos-start"
                          endLabelId="sentiment-pos-end"
                          labelA="Positive"
                          labelB=""
                          labelC=""
                          keyA="positive_posts"
                          keyB=""
                          keyC=""
                          colorA={{ area: "rgba(65, 200, 120, 0.18)", line: "#41c878" }}
                          colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          legendColorA="#41c878"
                        />
                      </div>
                      <div>
                        <p className="volume-period-label" style={{ marginBottom: "6px" }}>Negative Posts</p>
                        <VolumeChart
                          series={visibleFeatureSentimentSeries}
                          modeLabel={year === "overall" ? "overall" : year}
                          startLabelId="sentiment-neg-start"
                          endLabelId="sentiment-neg-end"
                          labelA="Negative"
                          labelB=""
                          labelC=""
                          keyA="negative_posts"
                          keyB=""
                          keyC=""
                          colorA={{ area: "rgba(255,77,103,0.22)", line: "#ff4d67" }}
                          colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          legendColorA="#ff4d67"
                        />
                      </div>
                      <div>
                        <p className="volume-period-label" style={{ marginBottom: "6px" }}>Neutral Posts</p>
                        <VolumeChart
                          series={visibleFeatureSentimentSeries}
                          modeLabel={year === "overall" ? "overall" : year}
                          startLabelId="sentiment-neu-start"
                          endLabelId="sentiment-neu-end"
                          labelA="Neutral"
                          labelB=""
                          labelC=""
                          keyA="neutral_posts"
                          keyB=""
                          keyC=""
                          colorA={{ area: "rgba(126,149,186,0.22)", line: "#7e95ba" }}
                          colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                          legendColorA="#7e95ba"
                        />
                      </div>
                    </div>
                  </article>

                  <article className="panel">
                    <h3>Word Popularity by Time</h3>
                    <form
                      className="button-row"
                      style={{ marginBottom: "8px" }}
                      onSubmit={(e) => {
                        e.preventDefault();
                        const trimmed = wordInput.trim().toLowerCase();
                        if (trimmed) setWordQuery(trimmed);
                      }}
                    >
                      <input
                        value={wordInput}
                        onChange={(e) => setWordInput(e.target.value)}
                        placeholder="Enter word"
                        style={{
                          background: "#171c28",
                          border: "1px solid rgba(255,255,255,0.12)",
                          color: "#d8deee",
                          borderRadius: "999px",
                          padding: "6px 10px",
                          fontSize: "0.8rem",
                          minWidth: "140px"
                        }}
                      />
                      <button className="filter-btn" type="submit">Show</button>
                    </form>
                    <VolumeChart
                      series={featureData?.word_popularity?.timeline || []}
                      modeLabel={year === "overall" ? "overall" : year}
                      startLabelId="word-pop-start"
                      endLabelId="word-pop-end"
                      labelA={wordQuery || "Word"}
                      labelB=""
                      labelC=""
                      keyA={wordQuery || "missing_word"}
                      keyB=""
                      keyC=""
                      colorA={{ area: "rgba(255,138,138,0.2)", line: "#ff8a8a" }}
                      colorB={{ area: "rgba(255,77,103,0.0)", line: "transparent" }}
                      colorC={{ area: "rgba(125,61,69,0.0)", line: "transparent" }}
                    />
                  </article>
                </section>

                <section className="bottom-grid">
                  <article className="panel">
                    <h3>Top Keywords (Filtered)</h3>
                    <div className="bar-list">
                      {(data?.top_keywords || []).length ? (
                        (data?.top_keywords || []).map((row) => (
                          <div key={row.label} className="bar-row">
                            <p>{row.label}</p>
                            <div className="track">
                              <div
                                className="fill"
                                style={{
                                  width: `${Math.max(
                                    (Number(row.value || 0) /
                                      Math.max(...(data?.top_keywords || []).map((r) => Number(r.value || 0)), 1)) *
                                      100,
                                    4
                                  )}%`
                                }}
                              />
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
                    <h3>Controversial Topics</h3>
                    <div className="bar-list">
                      {controversialTopics.length ? (
                        controversialTopics.map((row) => (
                          <div key={row.topic} className="bar-row">
                            <p>{row.topic}</p>
                            <div className="track">
                              <div
                                className="fill"
                                style={{ width: `${Math.max((Number(row.controversy_score || 0) / maxControversyScore) * 100, 6)}%` }}
                              />
                            </div>
                            <p>{Number(row.controversy_score || 0).toFixed(2)}</p>
                          </div>
                        ))
                      ) : (
                        <p>No controversial topic data.</p>
                      )}
                    </div>
                  </article>
                </section>

                <section className="analytics-grid" style={{ gridTemplateColumns: "1fr" }}>
                  <article className="panel">
                    <h3>Trend Saturation Monitor</h3>
                    {trendSummary.length ? (
                      <div style={{ display: "grid", gap: "16px" }}>
                        {trendSummary.slice(0, 8).map((row) => {
                          const topic = row.topic;
                          const series = trendSeriesByWord?.[topic] || [];
                          return (
                            <div
                              key={topic}
                              style={{
                                display: "grid",
                                gridTemplateColumns: "150px 1fr 70px",
                                gap: "12px",
                                alignItems: "center"
                              }}
                            >
                              <div style={{ color: "#d8deee", fontSize: "0.95rem", fontWeight: 600 }}>
                                {topic}
                              </div>
                              <div style={{ minWidth: 0 }}>
                                <VolumeChart
                                  series={series}
                                  modeLabel={year === "overall" ? "overall" : year}
                                  startLabelId={`trend-inline-start-${topic}`}
                                  endLabelId={`trend-inline-end-${topic}`}
                                  labelA={topic}
                                  labelB=""
                                  labelC=""
                                  keyA="value"
                                  keyB=""
                                  keyC=""
                                  colorA={{ area: "rgba(255,138,138,0.14)", line: "#ff8a8a" }}
                                  colorB={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                                  colorC={{ area: "rgba(0,0,0,0)", line: "transparent" }}
                                  legendColorA="#ff8a8a"
                                  height={140}
                                  wrapHeight={160}
                                  hideLegend
                                  hidePeriodLabel
                                  hideRange
                                />
                              </div>
                              <div style={{ color: "#cdd5e7", fontSize: "0.9rem", textAlign: "right" }}>
                                {row.status}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      <p>No trend saturation data.</p>
                    )}
                  </article>
                </section>
              </>
            ) : null}
          </div>
        </div>
      </main>

      <div className="footer-note">Reddit dashboard frame updated to analytics-board style.</div>
    </>
  );
}

