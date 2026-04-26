import React from "react";
import { NavLink } from "react-router-dom";

export default function TopNav({ brandTitle, brandAccent }) {
  return (
    <nav className="top-nav">
      <div className="brand">
        {brandTitle} <span>{brandAccent}</span>
      </div>
      <div className="nav-links">
        <NavLink to="/" end className={({ isActive }) => (isActive ? "active" : undefined)}>
          Home
        </NavLink>
        <NavLink to="/reddit" className={({ isActive }) => (isActive ? "active" : undefined)}>
          Reddit
        </NavLink>
        <NavLink to="/bluesky" className={({ isActive }) => (isActive ? "active" : undefined)}>
          Bluesky
        </NavLink>
        <NavLink to="/comparison" className={({ isActive }) => (isActive ? "active" : undefined)}>
          Comparison
        </NavLink>
      </div>
    </nav>
  );
}

