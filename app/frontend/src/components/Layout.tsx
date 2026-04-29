import { NavLink } from "react-router-dom";
import type { PropsWithChildren } from "react";

const navItems = [
  { to: "/", label: "Home" },
  { to: "/sentiment-analysis", label: "Sentiment Analysis" },
  { to: "/trending-topics", label: "Trending Topics by Time" },
  { to: "/word-popularity", label: "Word Popularity by Time" },
  { to: "/action-recommendor", label: "Action Recommendor" },
  { to: "/controversial-topics", label: "Controversial Topics" },
  { to: "/cross-links", label: "URLs Linking Bsky to Reddit" },
  { to: "/trend-saturation", label: "Trend Saturation Monitor" },
];

export function Layout({ children }: PropsWithChildren) {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <h2>Bluesky Intel</h2>
        <p className="sidebar-subtitle">Live social pulse dashboard</p>
        <nav>
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}
              end={item.to === "/"}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="content">
        <header className="topbar">
          <div>
            <h1>Analytics Command Center</h1>
            <p>10-minute windowed insights across ingestion, sentiment, and trends</p>
          </div>
          <div className="status-pill">Blue Theme</div>
        </header>
        {children}
      </main>
    </div>
  );
}

