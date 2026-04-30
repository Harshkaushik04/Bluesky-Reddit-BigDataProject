import React, { useMemo } from "react";

function formatBucket(bucket) {
  const dt = new Date(`${bucket.length === 7 ? `${bucket}-01` : bucket}T00:00:00`);
  if (Number.isNaN(dt.getTime())) return bucket;
  if (bucket.length === 7) return dt.toLocaleDateString(undefined, { month: "short", year: "2-digit" });
  return dt.toLocaleDateString(undefined, { month: "short", day: "2-digit" });
}

function sentimentColor(value, count, minValue, maxValue) {
  if (!count) return "rgba(255,255,255,0.05)";
  const span = maxValue - minValue;
  const normalized = span === 0 ? 0.5 : Math.max(0, Math.min(1, (value - minValue) / span));
  const blue = { r: 37, g: 99, b: 235 };
  const neutral = { r: 241, g: 245, b: 249 };
  const red = { r: 239, g: 68, b: 68 };
  const start = normalized < 0.5 ? blue : neutral;
  const end = normalized < 0.5 ? neutral : red;
  const local = normalized < 0.5 ? normalized * 2 : (normalized - 0.5) * 2;
  const r = Math.round(start.r + (end.r - start.r) * local);
  const g = Math.round(start.g + (end.g - start.g) * local);
  const b = Math.round(start.b + (end.b - start.b) * local);
  return `rgb(${r}, ${g}, ${b})`;
}

export default function TopicSentimentHeatmap({ data }) {
  const rows = data?.rows || [];
  const buckets = data?.buckets || [];
  const sentimentRange = useMemo(() => {
    const values = rows.flatMap((row) =>
      (row.cells || [])
        .filter((cell) => Number(cell.count || 0) > 0)
        .map((cell) => Number(cell.avg_sentiment || 0))
    );
    if (!values.length) return { min: -1, max: 1 };
    return { min: Math.min(...values), max: Math.max(...values) };
  }, [rows]);

  if (!rows.length || !buckets.length) {
    return <p>No topic sentiment heatmap data available.</p>;
  }

  return (
    <div className="sentiment-heatmap">
      <div className="heatmap-body">
        <div className="heatmap-scale" aria-label="Sentiment color scale">
          <span>{sentimentRange.max.toFixed(3)}</span>
          <div className="heatmap-scale-bar" />
          <span>{sentimentRange.min.toFixed(3)}</span>
        </div>

        <div className="heatmap-scroll">
          <div
            className="heatmap-grid"
            style={{ gridTemplateColumns: `120px repeat(${buckets.length}, minmax(42px, 1fr))` }}
          >
            <div className="heatmap-corner">Topic</div>
            {buckets.map((bucket) => (
              <div className="heatmap-date" key={bucket}>
                {formatBucket(bucket)}
              </div>
            ))}

            {rows.map((row) => (
              <React.Fragment key={row.topic}>
                <div className="heatmap-topic">{row.topic}</div>
                {(row.cells || []).map((cell) => (
                  <div
                    className="heatmap-cell"
                    key={`${row.topic}-${cell.bucket}`}
                    style={{
                      background: sentimentColor(
                        Number(cell.avg_sentiment || 0),
                        Number(cell.count || 0),
                        sentimentRange.min,
                        sentimentRange.max
                      )
                    }}
                    title={`${row.topic} in ${formatBucket(cell.bucket)}: sentiment ${Number(cell.avg_sentiment || 0).toFixed(3)}, mentions ${cell.count || 0}`}
                  />
                ))}
              </React.Fragment>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
