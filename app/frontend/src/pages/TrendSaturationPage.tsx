import { useState } from "react";
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";

type Response = { data: Record<string, Record<string, number>[]> };
const colors = ["#1d7cff", "#00d4ff", "#5ba6ff", "#0ec9ff", "#80d8ff", "#3f8cff"];

export function TrendSaturationPage() {
  const now = new Date();
  const [from, setFrom] = useState(new Date(now.getTime() - 60 * 86400000).toISOString().slice(0, 16));
  const [to, setTo] = useState(now.toISOString().slice(0, 16));
  const [numTopics, setNumTopics] = useState(10);
  const { data, loading, error } = usePolling(
    () =>
      postApi<Response, Record<string, string | number>>("/getTrendSaturation", {
        top_n_words: numTopics,
        "range-from": new Date(from).toISOString(),
        "range-to": new Date(to).toISOString(),
      }),
    [from, to, numTopics],
  );

  const chartRows: Record<string, string | number>[] = [];
  const byWord = data?.data ?? {};

  Object.entries(byWord).forEach(([word, points]) => {
    points.forEach((entry) => {
      const [time, val] = Object.entries(entry)[0];
      let row = chartRows.find((r) => r.time === time);
      if (!row) {
        row = { time };
        chartRows.push(row);
      }
      row[word] = val;
    });
  });

  return (
    <ChartPanel title="Trend Saturation Monitor" loading={loading} error={error}>
      <div className="controls">
        <label>
          Start
          <input type="datetime-local" value={from} onChange={(e) => setFrom(e.target.value)} />
        </label>
        <label>
          End
          <input type="datetime-local" value={to} onChange={(e) => setTo(e.target.value)} />
        </label>
        <label>
          Num topics
          <input
            type="number"
            min={1}
            max={50}
            value={numTopics}
            onChange={(e) => setNumTopics(Number(e.target.value))}
          />
        </label>
      </div>
      {chartRows.length === 0 ? (
        <p>No trend-saturation data found for this time window.</p>
      ) : (
        <ResponsiveContainer width="100%" height={420}>
          <LineChart data={chartRows}>
            <CartesianGrid stroke="#1f3b67" />
            <XAxis dataKey="time" stroke="#8cc7ff" tick={{ fontSize: 10 }} />
            <YAxis stroke="#8cc7ff" />
            <Tooltip />
            <Legend />
            {Object.keys(byWord).map((word, idx) => (
              <Line key={word} type="monotone" dataKey={word} stroke={colors[idx % colors.length]} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}
    </ChartPanel>
  );
}

