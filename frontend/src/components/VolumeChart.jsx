import React, { useMemo, useRef, useState } from "react";
import { formatNumber } from "../utils/format.js";

function buildAreaPath(points, height) {
  if (points.length === 0) return "";
  const start = `M ${points[0].x} ${height} L ${points[0].x} ${points[0].y}`;
  const mid = points.map((p) => `L ${p.x} ${p.y}`).join(" ");
  const end = `L ${points[points.length - 1].x} ${height} Z`;
  return `${start} ${mid} ${end}`;
}

function buildLinePath(points) {
  if (points.length === 0) return "";
  return `M ${points.map((p) => `${p.x} ${p.y}`).join(" L ")}`;
}

function formatBucketLabel(bucket, mode) {
  const dt = new Date(`${bucket}T00:00:00`);
  if (Number.isNaN(dt.getTime())) return bucket;
  if (mode === "overall") return dt.toLocaleDateString(undefined, { month: "short", year: "2-digit" });
  return dt.toLocaleDateString(undefined, { day: "2-digit", month: "short" });
}

export default function VolumeChart({
  series,
  modeLabel,
  startLabelId,
  endLabelId,
  colorA,
  colorB,
  colorC,
  labelA,
  labelB,
  labelC,
  keyA,
  keyB,
  keyC,
  legendColorA,
  legendColorB,
  legendColorC,
  height = 165,
  wrapHeight,
  hideLegend,
  hidePeriodLabel,
  hideRange
}) {
  const svgRef = useRef(null);
  const [tooltip, setTooltip] = useState(null);

  const chart = useMemo(() => {
    const safeSeries = series || [];
    const width = 760;
    const padding = { top: 8, right: 20, bottom: 18, left: 20 };
    const plotW = width - padding.left - padding.right;
    const plotH = height - padding.top - padding.bottom;

    const maxValue = Math.max(1, ...safeSeries.map((d) => Math.max(Number(d[keyA] || 0), Number(d[keyB] || 0), Number(d[keyC] || 0))));
    const n = safeSeries.length;
    const xStep = n > 1 ? plotW / (n - 1) : plotW;

    const toPoints = (key) =>
      safeSeries.map((d, i) => ({
        x: padding.left + i * xStep,
        y: padding.top + (1 - (Number(d[key] || 0) / maxValue)) * plotH
      }));

    const aPts = toPoints(keyA);
    const bPts = toPoints(keyB);
    const cPts = toPoints(keyC);
    const baseY = padding.top + plotH;

    const xTicks =
      n === 0
        ? ""
        : [...new Set([0, Math.floor((n - 1) / 2), Math.max(n - 1, 0)])]
            .map((idx) => {
              const point = aPts[idx];
              const row = safeSeries[idx];
              if (!point || !row) return "";
              const label = formatBucketLabel(row.bucket, modeLabel);
              return `<text x="${point.x}" y="${height - 1}" text-anchor="middle" fill="#9fb3d7" font-size="9">${label}</text>`;
            })
            .join("");

    const grid = [0.25, 0.5, 0.75]
      .map((ratio) => {
        const y = padding.top + plotH * ratio;
        return `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="rgba(255,255,255,0.08)" stroke-width="1" />`;
      })
      .join("");

    return {
      width,
      height,
      svg: `
        <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Volume chart">
          ${grid}
          <path d="${buildAreaPath(cPts, baseY)}" fill="${colorC?.area || 'transparent'}"></path>
          <path d="${buildAreaPath(bPts, baseY)}" fill="${colorB?.area || 'transparent'}"></path>
          <path d="${buildAreaPath(aPts, baseY)}" fill="${colorA?.area || 'transparent'}"></path>
          <path d="${buildLinePath(cPts)}" fill="none" stroke="${colorC?.line || 'transparent'}" stroke-width="1.8"></path>
          <path d="${buildLinePath(bPts)}" fill="none" stroke="${colorB?.line || 'transparent'}" stroke-width="1.8"></path>
          <path d="${buildLinePath(aPts)}" fill="none" stroke="${colorA?.line || 'transparent'}" stroke-width="1.8"></path>
          ${xTicks}
        </svg>
      `
    };
  }, [series, modeLabel, keyA, keyB, keyC, colorA, colorB, colorC, height]);

  const onMove = (event) => {
    if (!svgRef.current || !series || series.length === 0) return;
    const rect = svgRef.current.getBoundingClientRect();
    const relativeX = Math.max(0, Math.min(event.clientX - rect.left, rect.width));
    const normalized = rect.width <= 0 ? 0 : relativeX / rect.width;
    const idx = Math.max(0, Math.min(series.length - 1, Math.round(normalized * (series.length - 1))));
    const point = series[idx];
    setTooltip({
      x: Math.min(Math.max(relativeX + 12, 8), rect.width - 150),
      y: Math.max(event.clientY - rect.top - 76, 8),
      bucket: point.bucket,
      a: point[keyA],
      b: point[keyB],
      c: point[keyC],
      total: point.total || Number(point[keyA] || 0) + Number(point[keyB] || 0) + Number(point[keyC] || 0)
    });
  };

  const onLeave = () => setTooltip(null);

  return (
    <>
      {!hideLegend && (
        <div className="volume-legend">
          {labelA && (
            <span className="legend-item" style={{ "--dot-color": legendColorA || colorA?.line || "var(--accent)" }}>
              {labelA}
            </span>
          )}
          {labelB && (
            <span className="legend-item" style={{ "--dot-color": legendColorB || colorB?.line || "var(--accent)" }}>
              {labelB}
            </span>
          )}
          {labelC && (
            <span className="legend-item" style={{ "--dot-color": legendColorC || colorC?.line || "var(--accent)" }}>
              {labelC}
            </span>
          )}
        </div>
      )}

      {!hidePeriodLabel && (
        <p className="volume-period-label">Period: {modeLabel === "overall" ? "overall years" : modeLabel}</p>
      )}

      <div className="volume-chart-wrap" style={wrapHeight ? { height: wrapHeight } : {}}>
        <div
          ref={svgRef}
          onMouseMove={onMove}
          onMouseEnter={onMove}
          onMouseLeave={onLeave}
          dangerouslySetInnerHTML={{ __html: chart.svg }}
          style={{ width: "100%", height: "100%" }}
        />
        {tooltip ? (
          <div className="chart-tooltip" style={{ left: tooltip.x, top: tooltip.y }}>
            <div>
              <strong>{tooltip.bucket}</strong>
            </div>
            {labelA && (
              <div>
                {labelA}: {formatNumber(tooltip.a)}
              </div>
            )}
            {labelB && (
              <div>
                {labelB}: {formatNumber(tooltip.b)}
              </div>
            )}
            {labelC && (
              <div>
                {labelC}: {formatNumber(tooltip.c)}
              </div>
            )}
            <div>Total: {formatNumber(tooltip.total)}</div>
          </div>
        ) : null}
      </div>

      {!hideRange && (
        series && series.length ? (
          <div className="volume-range">
            <span id={startLabelId}>Start: {series[0].bucket}</span>
            <span id={endLabelId}>End: {series[series.length - 1].bucket}</span>
          </div>
        ) : (
          <div className="volume-range">
            <span id={startLabelId}>Start: -</span>
            <span id={endLabelId}>End: -</span>
          </div>
        )
      )}
    </>
  );
}

