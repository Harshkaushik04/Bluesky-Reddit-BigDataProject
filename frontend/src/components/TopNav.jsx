import React from "react";
import { NavLink } from "react-router-dom";

export default function TopNav({ brandTitle, brandAccent }) {
  return (
    <nav className="top-nav">
      <div className="brand">
        {brandTitle} <span>{brandAccent}</span>
      </div>
    </nav>
  );
}

