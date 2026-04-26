import React from "react";
import TopNav from "../components/TopNav.jsx";
import usePageTheme from "../hooks/usePageTheme.js";

export default function ComparisonPage() {
  usePageTheme("comparison");

  return (
    <>
      <TopNav brandTitle="AI528" brandAccent="Platform Comparison" />

      <main className="dashboard-wrap">
        <section className="filter-strip">
          <div className="filter-box">Filter: Date Range (Placeholder)</div>
          <div className="filter-box">Segment: Topic / Region</div>
          <div className="filter-box">Metric Group Selector</div>
        </section>

        <section className="hero-board">
          <div className="hero-head">
            <h1>REDDIT vs BLUESKY</h1>
            <span className="tag">Theme: Red + Blue Gradient</span>
          </div>
          <p className="hero-sub">
            Comparative board template ready for side-by-side KPI faceoff and trend overlays.
          </p>
        </section>

        <section className="kpi-grid">
          <article className="kpi">
            <p className="kpi-label">Total Mentions Gap</p>
            <p className="kpi-value">+18%</p>
            <p className="kpi-note">Reddit lead</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Sentiment Delta</p>
            <p className="kpi-value">+0.14</p>
            <p className="kpi-note">Bluesky lead</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Topic Overlap</p>
            <p className="kpi-value">63%</p>
            <p className="kpi-note">Shared themes</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Engagement Ratio</p>
            <p className="kpi-value">1.37x</p>
            <p className="kpi-note">Reddit/Bluesky</p>
          </article>
          <article className="kpi">
            <p className="kpi-label">Volatility Index</p>
            <p className="kpi-value">42.8</p>
            <p className="kpi-note">Cross-platform drift</p>
          </article>
        </section>

        <section className="analytics-grid">
          <article className="panel">
            <h3>Overlap Geography</h3>
            <div className="placeholder-map" />
          </article>
          <article className="panel">
            <h3>Contribution Split</h3>
            <div className="donut" />
            <div className="legend">
              <span>Reddit</span>
              <span>Bluesky</span>
            </div>
          </article>
          <article className="panel">
            <h3>Trend Overlay</h3>
            <div className="placeholder-line" />
          </article>
        </section>

        <section className="bottom-grid">
          <article className="panel">
            <h3>Top Shared Topics</h3>
            <div className="bar-list" />
          </article>
          <article className="panel">
            <h3>Topic Divergence</h3>
            <div className="bar-list" />
          </article>
          <article className="panel">
            <h3>Category Mix Treemap</h3>
            <div className="placeholder-tree" />
          </article>
        </section>
      </main>

      <div className="footer-note">Comparison dashboard frame updated to analytics-board style.</div>
    </>
  );
}

