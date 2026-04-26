import React, { useMemo } from "react";
import { formatNumber } from "../utils/format.js";

const DEFAULT_COLORS = {
  link: "#ff6b35",
  text: "#ff304a",
  poll: "#a855f7",
  video: "#ff8a8a",
  image: "#ff4d67",
  gallery: "#f59e0b",
  crosspost: "#22c55e",
  other: "#7d3d45"
};
const FALLBACK = ["#f43f5e", "#f97316", "#f59e0b", "#84cc16", "#10b981", "#06b6d4", "#3b82f6", "#8b5cf6"];

export default function PostTypePie({ split }) {
  const segments = useMemo(() => {
    if (!split || split.length === 0) return "conic-gradient(#4b5563 0 100%)";
    let start = 0;
    const parts = split.map((item, idx) => {
      const pct = Number(item.percent || 0);
      const end = start + pct;
      const color = DEFAULT_COLORS[item.label] || FALLBACK[idx % FALLBACK.length];
      const seg = `${color} ${start}% ${end}%`;
      start = end;
      return seg;
    });
    return `conic-gradient(${parts.join(", ")})`;
  }, [split]);

  return (
    <>
      <div className="post-type-pie" style={{ background: segments }} />
      <div className="post-type-list">
        {(split || []).map((item, idx) => {
          const color = DEFAULT_COLORS[item.label] || FALLBACK[idx % FALLBACK.length];
          return (
            <div key={item.label} className="post-type-row">
              <span className="post-type-color" style={{ background: color }} />
              <span>{item.label}</span>
              <span>{Number(item.percent || 0).toFixed(1)}%</span>
              <span>{formatNumber(item.value || 0)}</span>
            </div>
          );
        })}
      </div>
    </>
  );
}

