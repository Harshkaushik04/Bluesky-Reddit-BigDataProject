import React from "react";
import { Link } from "react-router-dom";
import TopNav from "../components/TopNav.jsx";
import usePageTheme from "../hooks/usePageTheme.js";

export default function HomePage() {
  usePageTheme("comparison");

  return (
    <>
      <TopNav brandTitle="AI528" brandAccent="Dashboard Hub" />

      <main className="dashboard-wrap">
        <section className="hero-board">
          <div className="hero-head">
            <h1>SOCIAL ANALYTICS DASHBOARD</h1>
            <span className="tag">Netflix-style board</span>
          </div>
          <p className="hero-sub">
            Choose a page to open a themed analytics frame. Data widgets can be wired next.
          </p>
        </section>

        <section className="bottom-grid">
          <Link to="/reddit" className="panel">
            <h3>1) Reddit Page</h3>
            <p className="hero-sub">Red themed dense analytics frame with KPI strip and insights panels.</p>
          </Link>
          <Link to="/bluesky" className="panel">
            <h3>2) Bluesky Page</h3>
            <p className="hero-sub">Blue themed analytics frame with identical structure for parity.</p>
          </Link>
          <Link to="/comparison" className="panel">
            <h3>3) Comparison Page</h3>
            <p className="hero-sub">Red-blue gradient frame for Reddit vs Bluesky comparisons.</p>
          </Link>
        </section>
      </main>

      <div className="footer-note">Dashboard hub ready.</div>
    </>
  );
}

