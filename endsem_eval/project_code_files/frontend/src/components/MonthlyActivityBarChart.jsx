import React, { useMemo } from "react";
import { formatNumber } from "../utils/format.js";

function formatMonthLabel(bucket) {
  const dt = new Date(`${bucket}-01T00:00:00`);
  if (Number.isNaN(dt.getTime())) return bucket;
  return dt.toLocaleDateString(undefined, { month: "short", year: "2-digit" });
}

function getWeekStart(date) {
  const weekStart = new Date(date);
  const day = weekStart.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  weekStart.setDate(weekStart.getDate() + diff);
  return weekStart;
}

function formatDateKey(date) {
  return date.toISOString().slice(0, 10);
}

function formatWeekLabel(bucket) {
  const dt = new Date(`${bucket}T00:00:00`);
  if (Number.isNaN(dt.getTime())) return bucket;
  return dt.toLocaleDateString(undefined, { month: "short", day: "2-digit" });
}

function getBucket(row, bucketMode) {
  const bucket = String(row.bucket || "");
  if (bucketMode === "week") {
    const dt = new Date(`${bucket}T00:00:00`);
    if (Number.isNaN(dt.getTime())) return null;
    return formatDateKey(getWeekStart(dt));
  }
  const monthKey = bucket.slice(0, 7);
  return /^\d{4}-\d{2}$/.test(monthKey) ? monthKey : null;
}

function formatBucketLabel(bucket, bucketMode) {
  return bucketMode === "week" ? formatWeekLabel(bucket) : formatMonthLabel(bucket);
}

export default function MonthlyActivityBarChart({
  series,
  valueKey,
  label,
  barClassName,
  color = "#ff8a8a",
  emptyMessage = "No activity data available.",
  bucketMode = "month"
}) {
  const monthlyRows = useMemo(() => {
    const byMonth = new Map();

    (series || []).forEach((row) => {
      const bucketKey = getBucket(row, bucketMode);
      if (!bucketKey) return;

      const current = byMonth.get(bucketKey) || { bucket: bucketKey, value: 0 };
      current.value += Number(row[valueKey] || 0);
      byMonth.set(bucketKey, current);
    });

    return Array.from(byMonth.values()).sort((a, b) => a.bucket.localeCompare(b.bucket));
  }, [series, valueKey, bucketMode]);

  const maxValue = useMemo(() => Math.max(1, ...monthlyRows.map((row) => row.value)), [monthlyRows]);

  if (!monthlyRows.length) {
    return <p>{emptyMessage}</p>;
  }

  return (
    <div className="monthly-chart">
      <div className="volume-legend">
        <span className="legend-item" style={{ "--dot-color": color }}>
          {label}
        </span>
      </div>

      <div className="monthly-bars" role="img" aria-label={`${bucketMode === "week" ? "Weekly" : "Monthly"} ${label.toLowerCase()} bar chart`}>
        {monthlyRows.map((row) => {
          const height = Math.max((row.value / maxValue) * 100, row.value > 0 ? 4 : 0);

          return (
            <div className="monthly-bar-group" key={row.bucket}>
              <div className="monthly-bar-plot">
                <div
                  className={`monthly-bar ${barClassName || ""}`}
                  style={{ height: `${height}%` }}
                  title={`${formatBucketLabel(row.bucket, bucketMode)} ${label.toLowerCase()}: ${formatNumber(row.value)}`}
                />
              </div>
              <p className="monthly-label">{formatBucketLabel(row.bucket, bucketMode)}</p>
              <p className="monthly-total">{formatNumber(row.value)}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
}
