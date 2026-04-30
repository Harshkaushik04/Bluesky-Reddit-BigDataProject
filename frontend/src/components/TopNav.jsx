import React from "react";
import { NavLink } from "react-router-dom";

export default function TopNav({ brandTitle, brandAccent }) {
  return (
    <nav className="top-nav">
      <div className="brand">
        {brandTitle} <span>{brandAccent}</span>
      </div>
      <div className="nav-links" style={{ display: "flex", gap: "24px", marginLeft: "auto", marginRight: "20px" }}>
        <NavLink to="/" end className={({ isActive }) => isActive ? "nav-item active-nav-item" : "nav-item"}>Reddit</NavLink>
        <NavLink to="/bluesky" className={({ isActive }) => isActive ? "nav-item active-nav-item" : "nav-item"}>Bluesky</NavLink>
        <NavLink to="/compare" className={({ isActive }) => isActive ? "nav-item active-nav-item" : "nav-item"}>Compare</NavLink>
      </div>
    </nav>
  );
}
