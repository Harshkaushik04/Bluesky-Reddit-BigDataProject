import React, { useEffect, useMemo, useState } from "react";
import TopNav from "../components/TopNav.jsx";
import FilterStrip from "../components/FilterStrip.jsx";
import usePageTheme from "../hooks/usePageTheme.js";
import { apiUrl, fetchJson } from "../utils/api.js";
import { formatNumber } from "../utils/format.js";
import VolumeChart from "../components/VolumeChart.jsx";
import PostTypePie from "../components/PostTypePie.jsx";
import MonthlyActivityBarChart from "../components/MonthlyActivityBarChart.jsx";
import TopicSentimentHeatmap from "../components/TopicSentimentHeatmap.jsx";

const API_BASE = apiUrl("/api/reddit/overview");
const COMMENTS_API_BASE = apiUrl("/api/reddit/comments/overview");
const FEATURES_API_BASE = apiUrl("/api/reddit/feature-insights");
const RECOMMEND_API_BASE = apiUrl("/api/reddit/action-recommend");
const RETRIEVE_API_BASE = apiUrl("/api/reddit/retrieve-posts");
const WHY_API_BASE = apiUrl("/api/reddit/why-sentiments");

function parseRecommendationGauge(result) {
  if (!result || result.startsWith("Error")) return null;
  const lower = result.toLowerCase();
  const avgScore = Number(result.match(/avg_score=(-?\d+(?:\.\d+)?)/)?.[1] ?? 0);
  const negFrac = Number(result.match(/neg_frac=(-?\d+(?:\.\d+)?)/)?.[1] ?? 0);
  const minScore = Number(result.match(/min_score=(-?\d+(?:\.\d+)?)/)?.[1] ?? 0);
  const shouldAvoid =
    lower.includes("do not post") || avgScore <= -0.05 || negFrac >= 0.4 || minScore <= -0.25;

  if (shouldAvoid) {
    return {
      status: "Avoid",
      detail: "High negative signal for this wording.",
      needle: 16,
      className: "avoid",
    };
  }
  if (avgScore >= 0.05 && negFrac < 0.25 && minScore >= 0) {
    return {
      status: "Safe to post",
      detail: "Positive signal with low negative pressure.",
      needle: 84,
      className: "safe",
    };
  }
  return {
    status: "Risky",
    detail: "Technically postable, but sentiment is weak or mixed.",
    needle: 50,
    className: "risky",
  };
}

function PostRecommendationGauge({ result }) {
  const gauge = parseRecommendationGauge(result);
  if (!gauge) return null;

  return (
    <div className={`recommendation-gauge ${gauge.className}`}>
      <div className="gauge-head">
        <div>
          <p className="gauge-label">Post Recommendation Gauge</p>
          <h4>{gauge.status}</h4>
        </div>
        <span>{gauge.detail}</span>
      </div>
      <div className="gauge-track">
        <div className="gauge-zone gauge-avoid">Avoid</div>
        <div className="gauge-zone gauge-risky">Risky</div>
        <div className="gauge-zone gauge-safe">Safe</div>
        <div className="gauge-needle" style={{ left: `${gauge.needle}%` }} />
      </div>
    </div>
  );
}

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

  const [actionSentence, setActionSentence] = useState("");
  const [actionResult, setActionResult] = useState(null);
  const [actionLoading, setActionLoading] = useState(false);

  const [whyWordInput, setWhyWordInput] = useState("");
  const [retrievedPosts, setRetrievedPosts] = useState([]);
  const [whyExplanation, setWhyExplanation] = useState(null);
  const [retrieveLoading, setRetrieveLoading] = useState(false);
  const [whyLoading, setWhyLoading] = useState(false);

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
    if (!data) setLoadingPosts(true); // Only show loading on first load, keep old data during refresh

    const fetchData = () => {
      fetchJson(apiUrl)
        .then((postsJson) => {
          if (!mounted) return;
          setData(postsJson);
        })
        .catch((e) => {
          if (mounted) setError((prev) => prev || e.message || "Failed to load posts data");
        })
        .finally(() => {
          if (mounted) setLoadingPosts(false);
        });
    };

    fetchData();
    const interval = setInterval(fetchData, 30000);

    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, [apiUrl]);

  useEffect(() => {
    if (activeSection !== "comments") return;
    let mounted = true;
    setError(null);
    if (!commentsData) setLoadingComments(true); // Don't flash loading if we already have data

    const fetchData = () => {
      fetchJson(commentsApiUrl)
        .then((commentsJson) => {
          if (!mounted) return;
          setCommentsData(commentsJson);
        })
        .catch((e) => {
          if (mounted) setError((prev) => prev || e.message || "Failed to load comments data");
        })
        .finally(() => {
          if (mounted) setLoadingComments(false);
        });
    };

    fetchData();
    const interval = setInterval(fetchData, 30000);

    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, [activeSection, commentsApiUrl]);

  useEffect(() => {
    if (activeSection !== "advanced" && activeSection !== "action-recommender") return;
    let mounted = true;
    setError(null);
    if (!featureData) setLoadingFeatures(true); // Don't flash loading if we already have data

    const fetchData = () => {
      fetchJson(featuresApiUrl)
        .then((featuresJson) => {
          if (!mounted) return;
          setFeatureData(featuresJson);
        })
        .catch((e) => {
          if (mounted) setError((prev) => prev || e.message || "Failed to load advanced insights");
        })
        .finally(() => {
          if (mounted) setLoadingFeatures(false);
        });
    };

    fetchData();
    const interval = setInterval(fetchData, 30000);

    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, [activeSection, featuresApiUrl]);

  const handleRecommendAction = async (e) => {
    e.preventDefault();
    if (!actionSentence.trim()) return;
    setActionLoading(true);
    setActionResult(null);
    try {
      const response = await fetch(RECOMMEND_API_BASE, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sentence: actionSentence }),
      });
      if (!response.ok) {
        throw new Error("Failed to get recommendation.");
      }
      const json = await response.json();
      setActionResult(json.response);
    } catch (err) {
      setActionResult("Error: " + err.message);
    } finally {
      setActionLoading(false);
    }
  };

  const handleRetrievePosts = async (e) => {
    e.preventDefault();
    if (!whyWordInput.trim()) return;
    setRetrieveLoading(true);
    setRetrievedPosts([]);
    setWhyExplanation(null);
    try {
      const response = await fetch(RETRIEVE_API_BASE, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ word: whyWordInput.trim(), limit: 5 }),
      });
      if (!response.ok) throw new Error("Retrieve failed");
      const json = await response.json();
      setRetrievedPosts(json.retrieved_posts || []);
      
      // Automatically get why-sentiments explanation after 2 seconds
      if (json.retrieved_posts && json.retrieved_posts.length > 0) {
        setTimeout(async () => {
          setWhyLoading(true);
          try {
            const whyResponse = await fetch(WHY_API_BASE, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ word: whyWordInput.trim(), retrieved_texts: json.retrieved_posts }),
            });
            if (whyResponse.ok) {
              const whyJson = await whyResponse.json();
              setWhyExplanation(whyJson.response);
            }
          } catch (whyErr) {
            console.error(whyErr);
          } finally {
            setWhyLoading(false);
          }
        }, 2000);
      }
    } catch (err) {
      console.error(err);
    } finally {
      setRetrieveLoading(false);
    }
  };

  const handleWhySentiment = async () => {
    if (!whyWordInput.trim() || retrievedPosts.length === 0) return;
    setWhyLoading(true);
    setWhyExplanation(null);
    try {
      const response = await fetch(WHY_API_BASE, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ word: whyWordInput.trim(), retrieved_texts: retrievedPosts }),
      });
      if (!response.ok) throw new Error("Explain failed");
      const json = await response.json();
      setWhyExplanation(json.response);
    } catch (err) {
      setWhyExplanation("Error: " + err.message);
    } finally {
      setWhyLoading(false);
    }
  };

  const kpis = data?.kpis || {};
  const split = data?.content_split || {};
  const commentsKpis = commentsData?.kpis || {};
  const sentimentKpis = featureData?.sentiment_kpis || {};
  const controversialTopics = featureData?.controversial_topics || [];
  const bestTopicsToPost = featureData?.best_topics_to_post || [];
  const topicsToAvoid = featureData?.topics_to_avoid || [];
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
              <button
                type="button"
                className={`side-btn ${activeSection === "action-recommender" ? "active" : ""}`}
                onClick={() => setActiveSection("action-recommender")}
              >
                Action Recommender
              </button>
            </div>
          </aside>

          <div className="dashboard-content">
            <button
              type="button"
              className="sidebar-toggle"
              aria-label={sidebarOpen ? "Hide sidebar" : "Show sidebar"}
              onClick={() => setSidebarOpen((v) => !v)}
            >
              <span className="dots" aria-hidden="true">
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
                    <span className="tag">Mode: Real-time</span>
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
                    <p className="kpi-note">Real-time update</p>
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

                <section className="analytics-grid" style={{ gridTemplateColumns: "1fr" }}>
                  <article className="panel">
                    <h3>Monthly Post Activity</h3>
                    <MonthlyActivityBarChart
                      series={postsSeries}
                      valueKey="posts"
                      label="Posts"
                      barClassName="monthly-bar-posts"
                      color="#ff8a8a"
                      emptyMessage="No monthly post data available."
                    />
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

                <section className="analytics-grid" style={{ gridTemplateColumns: "1fr" }}>
                  <article className="panel">
                    <h3>Weekly Comment Activity</h3>
                    <MonthlyActivityBarChart
                      series={commentsSeries}
                      valueKey="total_comments"
                      label="Comments"
                      barClassName="monthly-bar-comments"
                      color="#7d3d45"
                      bucketMode="week"
                      emptyMessage="No weekly comment data available."
                    />
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

                <section className="analytics-grid" style={{ gridTemplateColumns: "1fr" }}>
                  <article className="panel">
                    <h3>Sentiment Heatmap by Topic and Time</h3>
                    <TopicSentimentHeatmap data={featureData?.topic_sentiment_heatmap} />
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
                                gridTemplateColumns: "150px 1fr",
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
            ) : activeSection === "action-recommender" ? (
              <>
                <section className="hero-board">
                  <div className="hero-head">
                    <h1>ACTION RECOMMENDER</h1>
                    <span className="tag">Mode: Deterministic / LLM</span>
                  </div>
                  <p className="hero-sub">Enter a proposed post title to see if it should be posted based on historical sentiment.</p>
                </section>
                <section className="analytics-grid" style={{ gridTemplateColumns: "1fr" }}>
                  <article className="panel">
                    <h3>Propose a Post</h3>
                    <form onSubmit={handleRecommendAction} style={{ display: "flex", flexDirection: "column", gap: "16px", marginTop: "16px" }}>
                      <textarea
                        value={actionSentence}
                        onChange={(e) => setActionSentence(e.target.value)}
                        placeholder="Type your Reddit post title here..."
                        rows={4}
                        style={{
                          width: "100%",
                          padding: "12px",
                          borderRadius: "8px",
                          border: "1px solid rgba(255,255,255,0.1)",
                          background: "rgba(0,0,0,0.2)",
                          color: "#fff",
                          fontSize: "1rem",
                          fontFamily: "inherit",
                          resize: "vertical"
                        }}
                      />
                      <button
                        type="submit"
                        disabled={actionLoading || !actionSentence.trim()}
                        style={{
                          alignSelf: "flex-start",
                          padding: "10px 24px",
                          borderRadius: "8px",
                          border: "none",
                          background: "#ff4d67",
                          color: "#fff",
                          fontWeight: 600,
                          cursor: (actionLoading || !actionSentence.trim()) ? "not-allowed" : "pointer",
                          opacity: (actionLoading || !actionSentence.trim()) ? 0.6 : 1
                        }}
                      >
                        {actionLoading ? "Analyzing..." : "Recommend"}
                      </button>
                    </form>
                    
                    {actionResult && (
                      <div style={{
                        marginTop: "24px",
                        padding: "16px",
                        borderRadius: "8px",
                        background: actionResult.startsWith("Error") ? "rgba(255,77,103,0.1)" : "rgba(255,255,255,0.05)",
                        border: `1px solid ${actionResult.startsWith("Error") ? "rgba(255,77,103,0.3)" : "rgba(255,255,255,0.1)"}`,
                        whiteSpace: "pre-wrap"
                      }}>
                        <h4 style={{ margin: "0 0 12px 0", color: "#fff" }}>Recommendation Result:</h4>
                        <PostRecommendationGauge result={actionResult} />
                        <p style={{ margin: 0, lineHeight: 1.5, color: "#d8deee" }}>{actionResult}</p>
                      </div>
                    )}

                    <div className="recommendation-topic-block">
                      <h3>Best Topics to Post About</h3>
                      <div className="bar-list opportunity-bars">
                        {bestTopicsToPost.length ? (
                          bestTopicsToPost.map((row) => {
                            const maxScore = Math.max(...bestTopicsToPost.map((item) => Number(item.opportunity_score || 0)), 1);
                            return (
                              <div key={row.topic} className="bar-row opportunity-row">
                                <p>{row.topic}</p>
                                <div className="track">
                                  <div
                                    className="fill opportunity-fill"
                                    style={{ width: `${Math.max((Number(row.opportunity_score || 0) / maxScore) * 100, 6)}%` }}
                                  />
                                </div>
                                <p>{Number(row.opportunity_score || 0).toFixed(2)}</p>
                                <span>
                                  sentiment {Number(row.avg_sentiment || 0).toFixed(3)} · engagement {formatNumber(row.avg_engagement)}
                                </span>
                              </div>
                            );
                          })
                        ) : (
                          <p>{loadingFeatures ? "Loading best topics..." : "No positive topic opportunities found."}</p>
                        )}
                      </div>
                    </div>

                    <div className="recommendation-topic-block">
                      <h3>Topics to Avoid</h3>
                      <div className="bar-list opportunity-bars">
                        {topicsToAvoid.length ? (
                          topicsToAvoid.map((row) => {
                            const maxScore = Math.max(...topicsToAvoid.map((item) => Number(item.avoid_score || 0)), 1);
                            return (
                              <div key={row.topic} className="bar-row opportunity-row">
                                <p>{row.topic}</p>
                                <div className="track">
                                  <div
                                    className="fill avoid-fill"
                                    style={{ width: `${Math.max((Number(row.avoid_score || 0) / maxScore) * 100, 6)}%` }}
                                  />
                                </div>
                                <p>{Number(row.avoid_score || 0).toFixed(2)}</p>
                                <span>
                                  sentiment {Number(row.avg_sentiment || 0).toFixed(3)} · negative {Math.round(Number(row.negative_rate || 0) * 100)}% · controversy {Number(row.avg_controversy || 0).toFixed(2)}
                                </span>
                              </div>
                            );
                          })
                        ) : (
                          <p>{loadingFeatures ? "Loading topics to avoid..." : "No high-risk topics found."}</p>
                        )}
                      </div>
                    </div>
                  </article>
                </section>
              </>
            ) : activeSection === "why-sentiments" ? (
              <>
                <section className="hero-board">
                  <div className="hero-head">
                    <h1>WHY SENTIMENTS</h1>
                    <span className="tag">Mode: Vector Search + LLM</span>
                  </div>
                  <p className="hero-sub">Type a word to retrieve its exact context from the Reddit database using Qdrant, then let the LLM explain why it has that sentiment.</p>
                </section>
                <section className="analytics-grid" style={{ gridTemplateColumns: "1fr" }}>
                  <article className="panel">
                    <h3>Analyze Word Context</h3>
                    
                    <form onSubmit={handleRetrievePosts} style={{ display: "flex", gap: "12px", marginTop: "16px", flexWrap: "wrap" }}>
                      <input
                        type="text"
                        value={whyWordInput}
                        onChange={(e) => setWhyWordInput(e.target.value)}
                        placeholder="e.g. people, crypto, politics..."
                        style={{
                          flex: 1,
                          minWidth: "200px",
                          padding: "10px 14px",
                          borderRadius: "8px",
                          border: "1px solid rgba(255,255,255,0.1)",
                          background: "rgba(0,0,0,0.2)",
                          color: "#fff",
                          fontSize: "1rem"
                        }}
                      />
                      <button
                        type="submit"
                        disabled={retrieveLoading || !whyWordInput.trim()}
                        style={{
                          padding: "10px 20px",
                          borderRadius: "8px",
                          border: "1px solid #7c8cd8",
                          background: "rgba(124,140,216,0.1)",
                          color: "#7c8cd8",
                          fontWeight: 600,
                          cursor: (retrieveLoading || !whyWordInput.trim()) ? "not-allowed" : "pointer",
                          opacity: (retrieveLoading || !whyWordInput.trim()) ? 0.6 : 1
                        }}
                      >
                        {retrieveLoading ? "Searching VectorDB..." : "Retrieve Posts"}
                      </button>
                    </form>

                    {retrievedPosts.length > 0 && (
                      <div style={{ marginTop: "24px" }}>
                        <h4 style={{ color: "#d8deee", marginBottom: "12px" }}>Retrieved Context ({retrievedPosts.length} posts found)</h4>
                        <ul style={{ 
                          listStyle: "none", 
                          padding: 0, 
                          margin: "0 0 20px 0",
                          display: "flex",
                          flexDirection: "column",
                          gap: "8px"
                        }}>
                          {retrievedPosts.map((post, i) => (
                            <li key={i} style={{
                              padding: "10px 14px",
                              background: "rgba(0,0,0,0.2)",
                              borderRadius: "6px",
                              borderLeft: "3px solid #7c8cd8",
                              color: "#a4b1cd",
                              fontSize: "0.9rem",
                              lineHeight: 1.4
                            }}>
                              "{post}"
                            </li>
                          ))}
                        </ul>

                        <button
                          type="button"
                          onClick={handleWhySentiment}
                          disabled={whyLoading}
                          style={{
                            padding: "10px 24px",
                            borderRadius: "8px",
                            border: "none",
                            background: "#ff4d67",
                            color: "#fff",
                            fontWeight: 600,
                            cursor: whyLoading ? "not-allowed" : "pointer",
                            opacity: whyLoading ? 0.6 : 1
                          }}
                        >
                          {whyLoading ? "LLM Summarizing..." : "Explain Sentiment Context"}
                        </button>
                      </div>
                    )}

                    {whyExplanation && (
                      <div style={{
                        marginTop: "24px",
                        padding: "20px",
                        borderRadius: "8px",
                        background: whyExplanation.startsWith("Error") ? "rgba(255,77,103,0.1)" : "rgba(124,140,216,0.1)",
                        border: `1px solid ${whyExplanation.startsWith("Error") ? "rgba(255,77,103,0.3)" : "rgba(124,140,216,0.3)"}`,
                      }}>
                        <h4 style={{ margin: "0 0 12px 0", color: "#fff", display: "flex", alignItems: "center", gap: "8px" }}>
                          <span style={{ fontSize: "1.2rem" }}>✨</span> LLM Insight:
                        </h4>
                        <p style={{ margin: 0, lineHeight: 1.6, color: "#d8deee", fontSize: "0.95rem", whiteSpace: "pre-wrap" }}>
                          {whyExplanation}
                        </p>
                      </div>
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

